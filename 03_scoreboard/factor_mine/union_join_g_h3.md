# Factor mine action — `union_join_g_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ join_g, no 🚨

Cash book **+2.25%** ($10,226) · signal-only (no cash/fees) was +18.26%. Starts YES **9/17**. Fills 103 · skips 171 · realized $+11.76.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $95.31.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | MARA, LDI, BTBT | — | $63.95 | $10,435.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $63.95 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | $10,414.78 | -20.64 | TMC, DNN, NB | — | $48.18 | $10,525.00 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, NB×1 | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 |
| 2026-08-18 | -6.20 | $48.18 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, NB×1 | $10,391.53 | -133.47 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,308.79 | $10,355.26 | MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, NB×1 | 09:30 open · cash $48.18 (unchanged overnight, no fees) · equity $10,391.53 vs prior close $10,525.00 (-133.47) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×2 yday $3.19 → 09:30 $3.11 -0.16; NB×1 yday $4.81 → 09:30 $4.66 -0.15 |
| 2026-08-19 | -7.20 | $10,308.79 | MARA×1, LDI×13, BTBT×8, TMC×1, DNN×2, NB×1 | $10,355.41 | +0.15 | — | MARA, LDI, BTBT | $10,340.06 | $10,354.93 | TMC×1, DNN×2, NB×1 | 09:30 open · cash $10,308.79 (unchanged overnight, no fees) · equity $10,355.41 vs prior close $10,355.26 (+0.15) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×2 yday $3.15 → 09:30 $3.19 +0.08; NB×1 yday $4.53 → 09:30 $4.60 +0.07 |
| 2026-08-20 | +1.12 | $10,340.06 | TMC×1, DNN×2, NB×1 | $10,354.83 | -0.10 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | TMC, DNN, NB | $208.86 | $10,569.20 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | 09:30 open · cash $10,340.06 (unchanged overnight, no fees) · equity $10,354.83 vs prior close $10,354.93 (-0.10) because holdings re-marked: TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×2 yday $3.22 → 09:30 $3.20 -0.04; NB×1 yday $4.46 → 09:30 $4.45 -0.01 |
| 2026-08-21 | +3.25 | $208.86 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | $10,845.09 | +275.89 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $93.25 | $10,844.10 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $208.86 (unchanged overnight, no fees) · equity $10,845.09 vs prior close $10,569.20 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $93.25 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,973.48 | +129.38 | — | — | $93.25 | $10,815.08 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $93.25 (unchanged overnight, no fees) · equity $10,973.48 vs prior close $10,844.10 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 |
| 2026-08-25 | +1.80 | $93.25 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,884.94 | +69.86 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, ZURA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $113.96 | $10,846.89 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×55, OCUL×122, INSP×21, CRMD×162, RZLT×256, HCA×3, BMEA×828, ZURA×210 | 09:30 open · cash $93.25 (unchanged overnight, no fees) · equity $10,884.94 vs prior close $10,815.08 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 |
| 2026-08-26 | +2.02 | $113.96 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×55, OCUL×122, INSP×21, CRMD×162, RZLT×256, HCA×3, BMEA×828, ZURA×210 | $10,846.89 | -0.00 | — | — | $113.96 | $10,831.78 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×55, OCUL×122, INSP×21, CRMD×162, RZLT×256, HCA×3, BMEA×828, ZURA×210 | 09:30 open · cash $113.96 (unchanged overnight, no fees) · equity $10,846.89 vs prior close $10,846.89 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×55 yday $23.75 → 09:30 $23.75 +0.00; OCUL×122 yday $10.92 → 09:30 $10.92 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×162 yday $8.28 → 09:30 $8.28 +0.00; RZLT×256 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×828 yday $1.61 → 09:30 $1.61 +0.00; ZURA×210 yday $6.50 → 09:30 $6.50 +0.00 |
| 2026-08-27 | — | $113.96 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, MOS×55, OCUL×122, INSP×21, CRMD×162, RZLT×256, HCA×3, BMEA×828, ZURA×210 | $10,880.76 | +48.98 | CRK, SLI, GGB | AUPH, ARCT, AUTL, CRDL, CYPH | $146.84 | $10,786.21 | MOS×55, OCUL×122, INSP×21, CRMD×162, RZLT×256, HCA×3, BMEA×828, ZURA×210, CRK×2, SLI×13, GGB×7 | 09:30 open · cash $113.96 (unchanged overnight, no fees) · equity $10,880.76 vs prior close $10,831.78 (+48.98) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×55 yday $23.75 → 09:30 $24.84 +59.95; OCUL×122 yday $10.92 → 09:30 $10.79 -15.86; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×162 yday $8.28 → 09:30 $8.60 +51.84; RZLT×256 yday $5.29 → 09:30 $5.01 -71.68; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×828 yday $1.61 → 09:30 $1.75 +115.92; ZURA×210 yday $6.50 → 09:30 $6.13 -77.70 |
| 2026-08-28 | +0.75 | $146.84 | MOS×55, OCUL×122, INSP×21, CRMD×162, RZLT×256, HCA×3, BMEA×828, ZURA×210, CRK×2, SLI×13, GGB×7 | $10,814.48 | +28.27 | ANF, BZ, SEDG, SMTC, GRRR, URBN, VYX, SIMO | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, ZURA | $460.00 | $10,559.10 | CRK×2, SLI×13, GGB×7, ANF×9, BZ×72, SEDG×39, SMTC×8, GRRR×83, URBN×16, VYX×149, SIMO×4 | 09:30 open · cash $146.84 (unchanged overnight, no fees) · equity $10,814.48 vs prior close $10,786.21 (+28.27) because holdings re-marked: MOS×55 yday $24.16 → 09:30 $24.00 -8.80; OCUL×122 yday $10.77 → 09:30 $10.63 -17.08; INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×162 yday $8.39 → 09:30 $8.49 +16.20; RZLT×256 yday $5.04 → 09:30 $5.07 +7.68; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×828 yday $1.71 → 09:30 $1.74 +24.84; ZURA×210 yday $5.99 → 09:30 $6.02 +6.30; CRK×2 yday $14.50 → 09:30 $14.42 -0.16; SLI×13 yday $2.61 → 09:30 $2.60 -0.13; GGB×7 yday $4.46 → 09:30 $4.57 +0.77 |
| 2026-08-31 | -5.85 | $460.00 | CRK×2, SLI×13, GGB×7, ANF×9, BZ×72, SEDG×39, SMTC×8, GRRR×83, URBN×16, VYX×149, SIMO×4 | $10,295.63 | -263.47 | — | — | $460.00 | $10,254.97 | CRK×2, SLI×13, GGB×7, ANF×9, BZ×72, SEDG×39, SMTC×8, GRRR×83, URBN×16, VYX×149, SIMO×4 | 09:30 open · cash $460.00 (unchanged overnight, no fees) · equity $10,295.63 vs prior close $10,559.10 (-263.47) because holdings re-marked: CRK×2 yday $14.62 → 09:30 $14.56 -0.12; SLI×13 yday $2.64 → 09:30 $2.51 -1.69; GGB×7 yday $4.70 → 09:30 $4.55 -1.05; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BZ×72 yday $18.00 → 09:30 $17.89 -7.92; SEDG×39 yday $33.51 → 09:30 $31.50 -78.39; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×83 yday $15.66 → 09:30 $14.32 -111.22; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; VYX×149 yday $9.18 → 09:30 $9.06 -17.88; SIMO×4 yday $255.08 → 09:30 $246.79 -33.16 |
| 2026-09-01 | -6.30 | $460.00 | CRK×2, SLI×13, GGB×7, ANF×9, BZ×72, SEDG×39, SMTC×8, GRRR×83, URBN×16, VYX×149, SIMO×4 | $10,180.55 | -74.42 | — | CRK, SLI, GGB | $554.90 | $10,070.81 | ANF×9, BZ×72, SEDG×39, SMTC×8, GRRR×83, URBN×16, VYX×149, SIMO×4 | 09:30 open · cash $460.00 (unchanged overnight, no fees) · equity $10,180.55 vs prior close $10,254.97 (-74.42) because holdings re-marked: CRK×2 yday $14.51 → 09:30 $14.31 -0.40; SLI×13 yday $2.51 → 09:30 $2.70 +2.47; GGB×7 yday $4.55 → 09:30 $4.61 +0.42; ANF×9 yday $149.28 → 09:30 $142.47 -61.29; BZ×72 yday $17.90 → 09:30 $17.37 -38.16; SEDG×39 yday $31.27 → 09:30 $32.22 +37.05; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×83 yday $14.20 → 09:30 $15.05 +70.55; URBN×16 yday $81.09 → 09:30 $80.69 -6.40; VYX×149 yday $8.90 → 09:30 $8.40 -74.50; SIMO×4 yday $246.79 → 09:30 $247.53 +2.96 |
| 2026-09-02 | -3.83 | $554.90 | ANF×9, BZ×72, SEDG×39, SMTC×8, GRRR×83, URBN×16, VYX×149, SIMO×4 | $10,028.98 | -41.83 | — | ANF, BZ, SEDG, SMTC, GRRR, URBN, VYX, SIMO | $10,011.74 | $10,011.74 | — | 09:30 open · cash $554.90 (unchanged overnight, no fees) · equity $10,028.98 vs prior close $10,070.81 (-41.83) because holdings re-marked: ANF×9 yday $143.00 → 09:30 $142.00 -9.00; BZ×72 yday $17.17 → 09:30 $17.29 +8.64; SEDG×39 yday $31.80 → 09:30 $31.87 +2.73; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×83 yday $14.80 → 09:30 $14.75 -4.15; URBN×16 yday $80.69 → 09:30 $79.12 -25.12; VYX×149 yday $8.27 → 09:30 $8.30 +4.47; SIMO×4 yday $241.20 → 09:30 $240.09 -4.44 |
| 2026-09-03 | -0.90 | $10,011.74 | — | $10,011.74 | -0.00 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MMED, CTMX | — | $155.82 | $10,358.43 | ATRC×25, HRMY×30, CABA×382, VSTM×162, RVTY×9, CRK×79, MMED×54, CTMX×336 | 09:30 open · cash $10,011.74 · no holdings · equity $10,011.74 vs prior close $10,011.74 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $155.82 | ATRC×25, HRMY×30, CABA×382, VSTM×162, RVTY×9, CRK×79, MMED×54, CTMX×336 | $10,408.64 | +50.21 | NVAX, BVS, SLBT | — | $95.31 | $10,225.53 | ATRC×25, HRMY×30, CABA×382, VSTM×162, RVTY×9, CRK×79, MMED×54, CTMX×336, NVAX×2, BVS×1, SLBT×8 | 09:30 open · cash $155.82 (unchanged overnight, no fees) · equity $10,408.64 vs prior close $10,358.43 (+50.21) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; CABA×382 yday $3.57 → 09:30 $3.63 +22.92; VSTM×162 yday $8.02 → 09:30 $8.03 +1.62; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×79 yday $15.54 → 09:30 $15.45 -7.11; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; CTMX×336 yday $3.72 → 09:30 $3.73 +3.36 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 | — |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 1 | $4.05 | $0.04 | — | $59.85 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-12.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 2 | $3.24 | $0.07 | — | $53.30 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+0.3; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 1 | $5.07 | $0.05 | — | $48.18 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-4.7; leftover $7.99 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.18 | ▼ 09:30 equity $10,391.53 vs yday $10,525.00 (-133.47) | 09:30 open · cash $48.18 (unchanged overnight, no fees) · equity $10,391.53 vs prior close $10,525.00 (-133.47) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; TMC×1 yday $3.77 → 09:30 $3.72 -0.05; DNN×2 yday $3.19 → 09:30 $3.11 -0.16; NB×1 yday $4.81 → 09:30 $4.66 -0.15 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,246.11 | ▼ -0.12 after sell → book $10,389.46; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,420.14 | ▼ -69.50 after sell → book $10,387.37; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,660.54 | ▲ +23.38 after sell → book $10,385.29; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,890.45 | ▼ -14.65 after sell → book $10,383.20; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,230.07 | ▲ +97.12 after sell → book $10,380.86; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,397.64 | ▼ -83.63 after sell → book $10,378.73; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $9,136.48 | ▲ +471.89 after sell → book $10,358.55; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $10,308.79 | ▼ -66.33 after sell → book $10,356.38; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,308.79 | ▲ 09:30 equity $10,355.41 vs yday $10,355.26 (+0.15) | 09:30 open · cash $10,308.79 (unchanged overnight, no fees) · equity $10,355.41 vs prior close $10,355.26 (+0.15) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; TMC×1 yday $3.92 → 09:30 $3.93 +0.01; DNN×2 yday $3.15 → 09:30 $3.19 +0.08; NB×1 yday $4.53 → 09:30 $4.60 +0.07 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,317.59 | ▼ -0.31 after sell → book $10,355.30; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $10,328.86 | ▼ -1.08 after sell → book $10,355.13; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,340.06 | ▼ -0.94 after sell → book $10,354.97; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,340.06 | ▼ 09:30 equity $10,354.83 vs yday $10,354.93 (-0.10) | 09:30 open · cash $10,340.06 (unchanged overnight, no fees) · equity $10,354.83 vs prior close $10,354.93 (-0.10) because holdings re-marked: TMC×1 yday $3.97 → 09:30 $3.92 -0.05; DNN×2 yday $3.22 → 09:30 $3.20 -0.04; NB×1 yday $4.46 → 09:30 $4.45 -0.01 | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 1 | $3.92 | $0.06 | $-0.24 | $10,343.92 | ▼ -0.24 after sell → book $10,354.77; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 2 | $3.20 | $0.09 | $-0.24 | $10,350.23 | ▼ -0.24 after sell → book $10,354.68; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 1 | $4.45 | $0.07 | $-0.74 | $10,354.61 | ▼ -0.74 after sell → book $10,354.61; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,078.34 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.16 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.33 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,519.69 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,224.32 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.18 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,669.97 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.19 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.86 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.33 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.86 | ▲ 09:30 equity $10,845.09 vs yday $10,569.20 (+275.89) | 09:30 open · cash $208.86 (unchanged overnight, no fees) · equity $10,845.09 vs prior close $10,569.20 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $191.48 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $168.99 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.02 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $118.64 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $93.25 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.25 | ▲ 09:30 equity $10,973.48 vs yday $10,844.10 (+129.38) | 09:30 open · cash $93.25 (unchanged overnight, no fees) · equity $10,973.48 vs prior close $10,844.10 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.25 | ▲ 09:30 equity $10,884.94 vs yday $10,815.08 (+69.86) | 09:30 open · cash $93.25 (unchanged overnight, no fees) · equity $10,884.94 vs prior close $10,815.08 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,376.31 | ▲ +6.79 after sell → book $10,882.74; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,717.56 | ▲ +65.08 after sell → book $10,880.69; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.85 | $2.20 | $+8.03 | $4,008.06 | ▲ +8.03 after sell → book $10,878.49; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,243.85 | ▼ -59.59 after sell → book $10,875.56; vs 09:30 mark -2.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $6,647.59 | ▲ +125.61 after sell → book $10,873.35; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $8,054.13 | ▲ +130.33 after sell → book $10,871.21; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.91 | $9.67 | $+99.04 | $9,455.95 | ▲ +99.04 after sell → book $10,861.54; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,733.92 | ▲ +119.63 after sell → book $10,859.51; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $9,411.76 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.0; leftover $1341.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 122 | $10.92 | $2.36 | — | $8,077.17 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+10.4; leftover $1341.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $6,784.24 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+9.2; leftover $1341.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 162 | $8.28 | $2.48 | — | $5,440.41 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1341.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 256 | $5.23 | $3.30 | — | $4,098.22 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+10.7; leftover $1341.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,808.51 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+6.1; leftover $1341.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 828 | $1.62 | $10.68 | — | $1,456.46 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1341.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 210 | $6.38 | $2.71 | — | $113.96 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1341.74 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.96 | ▲ 09:30 equity $10,846.89 vs yday $10,846.89 (-0.00) | 09:30 open · cash $113.96 (unchanged overnight, no fees) · equity $10,846.89 vs prior close $10,846.89 (-0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; MOS×55 yday $23.75 → 09:30 $23.75 +0.00; OCUL×122 yday $10.92 → 09:30 $10.92 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×162 yday $8.28 → 09:30 $8.28 +0.00; RZLT×256 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×828 yday $1.61 → 09:30 $1.61 +0.00; ZURA×210 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.96 | ▲ 09:30 equity $10,880.76 vs yday $10,831.78 (+48.98) | 09:30 open · cash $113.96 (unchanged overnight, no fees) · equity $10,880.76 vs prior close $10,831.78 (+48.98) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; MOS×55 yday $23.75 → 09:30 $24.84 +59.95; OCUL×122 yday $10.92 → 09:30 $10.79 -15.86; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×162 yday $8.28 → 09:30 $8.60 +51.84; RZLT×256 yday $5.29 → 09:30 $5.01 -71.68; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×828 yday $1.61 → 09:30 $1.75 +115.92; ZURA×210 yday $6.50 → 09:30 $6.13 -77.70 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $130.37 | ▼ -0.96 after sell → book $10,880.57; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $160.73 | ▲ +7.88 after sell → book $10,880.23; vs 09:30 mark -0.34 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $184.54 | ▼ -1.17 after sell → book $10,879.94; vs 09:30 mark -0.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $210.61 | ▲ +0.69 after sell → book $10,879.62; vs 09:30 mark -0.32 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $240.63 | ▲ +4.63 after sell → book $10,879.24; vs 09:30 mark -0.38 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 2 | $14.09 | $0.29 | — | $212.16 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+1.1; leftover $34.38 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 13 | $2.59 | $0.38 | — | $178.11 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+4.2; leftover $34.38 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 7 | $4.42 | $0.33 | — | $146.84 | — | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=-8.6; leftover $34.38 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $146.84 | ▲ 09:30 equity $10,814.48 vs yday $10,786.21 (+28.27) | 09:30 open · cash $146.84 (unchanged overnight, no fees) · equity $10,814.48 vs prior close $10,786.21 (+28.27) because holdings re-marked: MOS×55 yday $24.16 → 09:30 $24.00 -8.80; OCUL×122 yday $10.77 → 09:30 $10.63 -17.08; INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×162 yday $8.39 → 09:30 $8.49 +16.20; RZLT×256 yday $5.04 → 09:30 $5.07 +7.68; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×828 yday $1.71 → 09:30 $1.74 +24.84; ZURA×210 yday $5.99 → 09:30 $6.02 +6.30; CRK×2 yday $14.50 → 09:30 $14.42 -0.16; SLI×13 yday $2.61 → 09:30 $2.60 -0.13; GGB×7 yday $4.46 → 09:30 $4.57 +0.77 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $24.00 | $2.18 | $-4.33 | $1,464.67 | ▼ -4.33 after sell → book $10,812.31; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 122 | $10.63 | $2.39 | $-40.12 | $2,759.14 | ▼ -40.12 after sell → book $10,809.92; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $62.10 | $2.07 | $+9.10 | $4,061.17 | ▲ +9.10 after sell → book $10,807.85; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 162 | $8.49 | $2.51 | $+29.03 | $5,434.03 | ▲ +29.03 after sell → book $10,805.33; vs 09:30 mark -2.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 256 | $5.07 | $3.36 | $-47.62 | $6,728.60 | ▼ -47.62 after sell → book $10,801.98; vs 09:30 mark -3.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $8,000.41 | ▼ -17.91 after sell → book $10,799.96; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 828 | $1.74 | $10.83 | $+77.85 | $9,430.30 | ▲ +77.85 after sell → book $10,789.13; vs 09:30 mark -10.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 210 | $6.02 | $2.75 | $-81.06 | $10,691.75 | ▼ -81.06 after sell → book $10,786.38; vs 09:30 mark -2.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,387.43 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1336.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 72 | $18.50 | $2.21 | — | $8,053.22 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1336.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 39 | $33.78 | $2.11 | — | $6,733.70 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1336.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $5,536.48 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1336.47 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 83 | $15.94 | $2.24 | — | $4,211.22 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1336.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $82.70 | $2.04 | — | $2,885.99 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1336.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 149 | $8.95 | $2.44 | — | $1,550.00 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; 🔵; ret5=-3.1; leftover $1336.47 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $272.00 | $2.00 | — | $460.00 | — | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; ⚪; ret5=-3.9; leftover $1336.47 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $460.00 | ▼ 09:30 equity $10,295.63 vs yday $10,559.10 (-263.47) | 09:30 open · cash $460.00 (unchanged overnight, no fees) · equity $10,295.63 vs prior close $10,559.10 (-263.47) because holdings re-marked: CRK×2 yday $14.62 → 09:30 $14.56 -0.12; SLI×13 yday $2.64 → 09:30 $2.51 -1.69; GGB×7 yday $4.70 → 09:30 $4.55 -1.05; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BZ×72 yday $18.00 → 09:30 $17.89 -7.92; SEDG×39 yday $33.51 → 09:30 $31.50 -78.39; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×83 yday $15.66 → 09:30 $14.32 -111.22; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; VYX×149 yday $9.18 → 09:30 $9.06 -17.88; SIMO×4 yday $255.08 → 09:30 $246.79 -33.16 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $460.00 | ▼ 09:30 equity $10,180.55 vs yday $10,254.97 (-74.42) | 09:30 open · cash $460.00 (unchanged overnight, no fees) · equity $10,180.55 vs prior close $10,254.97 (-74.42) because holdings re-marked: CRK×2 yday $14.51 → 09:30 $14.31 -0.40; SLI×13 yday $2.51 → 09:30 $2.70 +2.47; GGB×7 yday $4.55 → 09:30 $4.61 +0.42; ANF×9 yday $149.28 → 09:30 $142.47 -61.29; BZ×72 yday $17.90 → 09:30 $17.37 -38.16; SEDG×39 yday $31.27 → 09:30 $32.22 +37.05; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×83 yday $14.20 → 09:30 $15.05 +70.55; URBN×16 yday $81.09 → 09:30 $80.69 -6.40; VYX×149 yday $8.90 → 09:30 $8.40 -74.50; SIMO×4 yday $246.79 → 09:30 $247.53 +2.96 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 2 | $14.31 | $0.31 | $-0.16 | $488.30 | ▼ -0.16 after sell → book $10,180.23; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 13 | $2.70 | $0.41 | $+0.64 | $522.99 | ▲ +0.64 after sell → book $10,179.82; vs 09:30 mark -0.41 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 7 | $4.61 | $0.36 | $+0.64 | $554.90 | ▲ +0.64 after sell → book $10,179.46; vs 09:30 mark -0.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $554.90 | ▼ 09:30 equity $10,028.98 vs yday $10,070.81 (-41.83) | 09:30 open · cash $554.90 (unchanged overnight, no fees) · equity $10,028.98 vs prior close $10,070.81 (-41.83) because holdings re-marked: ANF×9 yday $143.00 → 09:30 $142.00 -9.00; BZ×72 yday $17.17 → 09:30 $17.29 +8.64; SEDG×39 yday $31.80 → 09:30 $31.87 +2.73; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×83 yday $14.80 → 09:30 $14.75 -4.15; URBN×16 yday $80.69 → 09:30 $79.12 -25.12; VYX×149 yday $8.27 → 09:30 $8.30 +4.47; SIMO×4 yday $241.20 → 09:30 $240.09 -4.44 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $142.00 | $2.04 | $-28.35 | $1,830.86 | ▼ -28.35 after sell → book $10,026.94; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 72 | $17.29 | $2.23 | $-91.55 | $3,073.52 | ▼ -91.55 after sell → book $10,024.72; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 39 | $31.87 | $2.13 | $-78.72 | $4,314.32 | ▼ -78.72 after sell → book $10,022.59; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $5,333.32 | ▼ -178.21 after sell → book $10,020.55; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 83 | $14.75 | $2.26 | $-103.27 | $6,555.31 | ▼ -103.27 after sell → book $10,018.29; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 16 | $79.12 | $2.06 | $-61.38 | $7,819.17 | ▼ -61.38 after sell → book $10,016.23; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 149 | $8.30 | $2.47 | $-101.76 | $9,053.40 | ▼ -101.76 after sell → book $10,013.76; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 4 | $240.09 | $2.02 | $-131.66 | $10,011.74 | ▼ -131.66 after sell → book $10,011.74; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,011.74 | ▲ 09:30 equity $10,011.74 vs yday $10,011.74 (-0.00) | 09:30 open · cash $10,011.74 · no holdings · equity $10,011.74 vs prior close $10,011.74 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $49.76 | $2.06 | — | $8,765.67 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1251.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $7,524.29 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1251.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 382 | $3.27 | $4.93 | — | $6,270.23 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1251.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 162 | $7.70 | $2.48 | — | $5,020.35 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1251.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,884.87 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1251.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 79 | $15.70 | $2.23 | — | $2,642.35 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1251.47 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 54 | $22.78 | $2.15 | — | $1,410.07 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1251.47 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 336 | $3.72 | $4.33 | — | $155.82 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1251.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.82 | ▲ 09:30 equity $10,408.64 vs yday $10,358.43 (+50.21) | 09:30 open · cash $155.82 (unchanged overnight, no fees) · equity $10,408.64 vs prior close $10,358.43 (+50.21) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; CABA×382 yday $3.57 → 09:30 $3.63 +22.92; VSTM×162 yday $8.02 → 09:30 $8.03 +1.62; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×79 yday $15.54 → 09:30 $15.45 -7.11; MMED×54 yday $23.76 → 09:30 $23.88 +6.48; CTMX×336 yday $3.72 → 09:30 $3.73 +3.36 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $134.79 | — | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $25.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $120.14 | — | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $25.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 8 | $3.07 | $0.27 | — | $95.31 | — | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $25.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
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
| 2026-08-17 | `DVN` | cash | leftover split 7.99 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 7.99 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 7.99 < 1 share @ 202.70 |
| 2026-08-17 | `TGB` | cash | leftover split 7.99 < 1 share @ 8.46 |
| 2026-08-17 | `ELF` | cash | leftover split 7.99 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.11 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.11 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.11 < 1 share @ 59.72 |
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
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-26 | `OCUL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CRMD` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RZLT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HCA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-26 | `MAIR` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 34.38 < 1 share @ 40.72 |
| 2026-08-27 | `ACMR` | cash | leftover split 34.38 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 34.38 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 34.38 < 1 share @ 925.74 |
| 2026-08-28 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 25.97 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 25.97 < 1 share @ 30.65 |
| 2026-09-04 | `DELL` | cash | leftover split 25.97 < 1 share @ 486.31 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 25 | 2026-09-03 @ $49.76 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1251.47 |
| `HRMY` | 30 | 2026-09-03 @ $41.31 | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1251.47 |
| `CABA` | 382 | 2026-09-03 @ $3.27 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1251.47 |
| `VSTM` | 162 | 2026-09-03 @ $7.70 | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1251.47 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1251.47 |
| `CRK` | 79 | 2026-09-03 @ $15.70 | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1251.47 |
| `MMED` | 54 | 2026-09-03 @ $22.78 | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1251.47 |
| `CTMX` | 336 | 2026-09-03 @ $3.72 | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1251.47 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $25.97 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $25.97 |
| `SLBT` | 8 | 2026-09-04 @ $3.07 | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $25.97 |
