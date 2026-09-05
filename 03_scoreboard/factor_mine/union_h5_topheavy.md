# Factor mine action — `union_h5_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+15.08%** ($11,508) · signal-only (no cash/fees) was +58.01%. Starts YES **15/17**. Fills 100 · skips 238 · realized $+1103.22.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $53.73.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $160.57 | $10,123.05 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $160.57 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | $10,109.78 | -13.27 | MARA, LDI, BTBT | — | $124.52 | $10,395.68 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9 | 09:30 open · cash $160.57 (unchanged overnight, no fees) · equity $10,109.78 vs prior close $10,123.05 (-13.27) because holdings re-marked: BTSG×66 yday $60.23 → 09:30 $59.65 -38.28; IREN×18 yday $44.76 → 09:30 $44.09 -12.06; TPG×16 yday $54.62 → 09:30 $55.29 +10.72; TGTX×17 yday $47.94 → 09:30 $47.27 -11.39; SLS×73 yday $12.36 → 09:30 $12.40 +2.92; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×1058 yday $0.90 → 09:30 $0.93 +31.74; TNDM×36 yday $23.13 → 09:30 $22.92 -7.56 |
| 2026-08-17 | +2.25 | $124.52 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9 | $10,380.01 | -15.67 | DVN, TMC, TGB, DNN, HNST | — | $41.59 | $10,388.13 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $124.52 (unchanged overnight, no fees) · equity $10,380.01 vs prior close $10,395.68 (-15.67) because holdings re-marked: BTSG×66 yday $61.71 → 09:30 $61.69 -1.32; IREN×18 yday $44.06 → 09:30 $45.23 +21.06; TPG×16 yday $53.03 → 09:30 $52.67 -5.76; TGTX×17 yday $48.74 → 09:30 $48.74 +0.00; SLS×73 yday $12.78 → 09:30 $12.78 +0.00; HIMS×28 yday $28.15 → 09:30 $28.14 -0.28; INO×1058 yday $1.09 → 09:30 $1.07 -21.16; TNDM×36 yday $22.72 → 09:30 $22.50 -7.92; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×14 yday $0.90 → 09:30 $0.91 +0.14; BTBT×9 yday $1.57 → 09:30 $1.52 -0.45 |
| 2026-08-18 | -6.20 | $41.59 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | $10,277.67 | -110.46 | — | — | $41.59 | $10,375.43 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $41.59 (unchanged overnight, no fees) · equity $10,277.67 vs prior close $10,388.13 (-110.46) because holdings re-marked: BTSG×66 yday $60.38 → 09:30 $60.00 -25.08; IREN×18 yday $44.90 → 09:30 $43.56 -24.12; TPG×16 yday $51.77 → 09:30 $51.77 +0.00; TGTX×17 yday $49.28 → 09:30 $49.28 +0.00; SLS×73 yday $13.00 → 09:30 $12.66 -24.82; HIMS×28 yday $28.61 → 09:30 $27.85 -21.28; INO×1058 yday $1.15 → 09:30 $1.14 -10.58; TNDM×36 yday $22.25 → 09:30 $22.16 -3.42; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.60 → 09:30 $1.54 -0.54; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 |
| 2026-08-19 | -7.20 | $41.59 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | $10,504.56 | +129.13 | — | — | $41.59 | $10,678.44 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $41.59 (unchanged overnight, no fees) · equity $10,504.56 vs prior close $10,375.43 (+129.13) because holdings re-marked: BTSG×66 yday $59.50 → 09:30 $60.15 +42.90; IREN×18 yday $42.00 → 09:30 $41.41 -10.53; TPG×16 yday $52.02 → 09:30 $52.26 +3.84; TGTX×17 yday $50.26 → 09:30 $51.62 +23.12; SLS×73 yday $13.10 → 09:30 $13.46 +26.28; HIMS×28 yday $27.39 → 09:30 $27.55 +4.48; INO×1058 yday $1.20 → 09:30 $1.22 +21.16; TNDM×36 yday $23.73 → 09:30 $24.20 +16.92; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×14 yday $0.86 → 09:30 $0.88 +0.31; BTBT×9 yday $1.45 → 09:30 $1.42 -0.27; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 |
| 2026-08-20 | +1.12 | $41.59 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | $10,599.55 | -78.89 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $121.79 | $10,821.22 | MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2, AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6 | 09:30 open · cash $41.59 (unchanged overnight, no fees) · equity $10,599.55 vs prior close $10,678.44 (-78.89) because holdings re-marked: BTSG×66 yday $59.33 → 09:30 $58.64 -45.54; IREN×18 yday $42.84 → 09:30 $42.46 -6.84; TPG×16 yday $53.18 → 09:30 $53.06 -1.92; TGTX×17 yday $51.69 → 09:30 $51.65 -0.68; SLS×73 yday $13.85 → 09:30 $13.84 -0.73; HIMS×28 yday $31.09 → 09:30 $30.66 -12.04; INO×1058 yday $1.30 → 09:30 $1.30 +0.00; TNDM×36 yday $23.46 → 09:30 $23.11 -12.60; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.40 → 09:30 $1.46 +0.50; DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 |
| 2026-08-21 | +3.25 | $121.79 | MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2, AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6 | $11,128.77 | +307.55 | ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT | $109.29 | $11,000.40 | DVN×1, TMC×2, TGB×1, DNN×3, HNST×2, AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10 | 09:30 open · cash $121.79 (unchanged overnight, no fees) · equity $11,128.77 vs prior close $10,821.22 (+307.55) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×14 yday $0.87 → 09:30 $0.87 -0.04; BTBT×9 yday $1.59 → 09:30 $1.66 +0.58; DVN×1 yday $49.30 → 09:30 $49.45 +0.15; TMC×2 yday $3.97 → 09:30 $4.10 +0.26; TGB×1 yday $8.69 → 09:30 $9.00 +0.31; DNN×3 yday $3.14 → 09:30 $3.23 +0.27; HNST×2 yday $4.96 → 09:30 $4.97 +0.02; AG×203 yday $21.19 → 09:30 $21.90 +144.13; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×43 yday $21.11 → 09:30 $21.75 +27.52; HDSN×155 yday $5.57 → 09:30 $5.67 +15.50; IAG×45 yday $20.50 → 09:30 $21.17 +30.15; KGC×30 yday $31.43 → 09:30 $32.17 +22.20; NFGC×511 yday $1.75 → 09:30 $1.79 +20.44; WPM×6 yday $150.25 → 09:30 $154.70 +26.70 |
| 2026-08-24 | -5.17 | $109.29 | DVN×1, TMC×2, TGB×1, DNN×3, HNST×2, AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10 | $11,149.50 | +149.10 | — | DVN, TMC, TGB, DNN, HNST | $196.13 | $10,895.06 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10 | 09:30 open · cash $109.29 (unchanged overnight, no fees) · equity $11,149.50 vs prior close $11,000.40 (+149.10) because holdings re-marked: DVN×1 yday $49.10 → 09:30 $48.84 -0.26; TMC×2 yday $4.79 → 09:30 $4.57 -0.44; TGB×1 yday $9.19 → 09:30 $9.26 +0.07; DNN×3 yday $3.50 → 09:30 $3.50 +0.00; HNST×2 yday $5.05 → 09:30 $5.05 +0.00; AG×203 yday $21.09 → 09:30 $21.47 +77.14; BHP×9 yday $97.03 → 09:30 $97.34 +2.79; CDE×43 yday $20.97 → 09:30 $21.26 +12.47; HDSN×155 yday $5.63 → 09:30 $5.69 +9.30; IAG×45 yday $21.14 → 09:30 $21.44 +13.50; KGC×30 yday $32.76 → 09:30 $33.21 +13.50; NFGC×511 yday $1.84 → 09:30 $1.86 +10.22; WPM×6 yday $157.78 → 09:30 $158.96 +7.08; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×5 yday $2.41 → 09:30 $2.36 -0.25; CRDL×7 yday $1.86 → 09:30 $1.87 +0.07; CYPH×10 yday $1.42 → 09:30 $1.83 +4.10 |
| 2026-08-25 | +1.80 | $196.13 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10 | $10,969.97 | +74.91 | MOS, OCUL, CRMD, RZLT, BMEA, NPWR | — | $47.20 | $10,917.14 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8 | 09:30 open · cash $196.13 (unchanged overnight, no fees) · equity $10,969.97 vs prior close $10,895.06 (+74.91) because holdings re-marked: AG×203 yday $20.57 → 09:30 $20.73 +32.48; BHP×9 yday $96.66 → 09:30 $95.95 -6.39; CDE×43 yday $20.49 → 09:30 $20.85 +15.48; HDSN×155 yday $5.57 → 09:30 $5.53 -6.20; IAG×45 yday $21.36 → 09:30 $21.63 +12.15; KGC×30 yday $32.47 → 09:30 $32.76 +8.70; NFGC×511 yday $1.90 → 09:30 $1.91 +5.11; WPM×6 yday $158.00 → 09:30 $160.00 +12.00; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; AUTL×5 yday $2.38 → 09:30 $2.32 -0.30; CRDL×7 yday $1.80 → 09:30 $1.90 +0.70; CYPH×10 yday $1.64 → 09:30 $1.70 +0.60 |
| 2026-08-26 | +2.02 | $47.20 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8 | $10,917.14 | +0.00 | — | — | $47.20 | $10,968.41 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8 | 09:30 open · cash $47.20 (unchanged overnight, no fees) · equity $10,917.14 vs prior close $10,917.14 (+0.00) because holdings re-marked: AG×203 yday $20.68 → 09:30 $20.68 +0.00; BHP×9 yday $96.05 → 09:30 $96.05 +0.00; CDE×43 yday $20.71 → 09:30 $20.71 +0.00; HDSN×155 yday $5.49 → 09:30 $5.49 +0.00; IAG×45 yday $21.48 → 09:30 $21.48 +0.00; KGC×30 yday $32.55 → 09:30 $32.55 +0.00; NFGC×511 yday $1.90 → 09:30 $1.90 +0.00; WPM×6 yday $158.25 → 09:30 $158.25 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; AUTL×5 yday $2.34 → 09:30 $2.34 +0.00; CRDL×7 yday $1.90 → 09:30 $1.90 +0.00; CYPH×10 yday $1.64 → 09:30 $1.64 +0.00; MOS×3 yday $23.75 → 09:30 $23.75 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×2 yday $8.28 → 09:30 $8.28 +0.00; RZLT×3 yday $5.29 → 09:30 $5.29 +0.00; BMEA×10 yday $1.61 → 09:30 $1.61 +0.00; NPWR×8 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $47.20 | AG×203, BHP×9, CDE×43, HDSN×155, IAG×45, KGC×30, NFGC×511, WPM×6, ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8 | $11,021.52 | +53.11 | RRC, CRK, SLI, ACMR, GGB, MT, MU | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $198.91 | $11,094.95 | ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8, RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1 | 09:30 open · cash $47.20 (unchanged overnight, no fees) · equity $11,021.52 vs prior close $10,968.41 (+53.11) because holdings re-marked: AG×203 yday $20.68 → 09:30 $20.63 -10.15; BHP×9 yday $96.05 → 09:30 $96.99 +8.46; CDE×43 yday $20.71 → 09:30 $21.00 +12.47; HDSN×155 yday $5.49 → 09:30 $5.51 +3.10; IAG×45 yday $21.48 → 09:30 $21.64 +7.20; KGC×30 yday $32.55 → 09:30 $32.90 +10.50; NFGC×511 yday $1.90 → 09:30 $2.00 +51.10; WPM×6 yday $158.25 → 09:30 $160.93 +16.08; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; AUTL×5 yday $2.34 → 09:30 $2.41 +0.35; CRDL×7 yday $1.90 → 09:30 $2.03 +0.91; CYPH×10 yday $1.64 → 09:30 $1.60 -0.40; MOS×3 yday $23.75 → 09:30 $24.84 +3.27; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×2 yday $8.28 → 09:30 $8.60 +0.64; RZLT×3 yday $5.29 → 09:30 $5.01 -0.84; BMEA×10 yday $1.61 → 09:30 $1.75 +1.40; NPWR×8 yday $2.02 → 09:30 $1.93 -0.72 |
| 2026-08-28 | +0.75 | $198.91 | ARCT×1, AUTL×5, CRDL×7, CYPH×10, MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8, RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1 | $11,162.50 | +67.55 | BHVN, BZ, CAPR | ARCT, AUTL, CRDL, CYPH | $122.47 | $11,199.29 | MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8, RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1, BHVN×3, BZ×2, CAPR×5 | 09:30 open · cash $198.91 (unchanged overnight, no fees) · equity $11,162.50 vs prior close $11,094.95 (+67.55) because holdings re-marked: ARCT×1 yday $15.83 → 09:30 $15.74 -0.09; AUTL×5 yday $2.33 → 09:30 $2.32 -0.05; CRDL×7 yday $2.14 → 09:30 $2.09 -0.35; CYPH×10 yday $1.63 → 09:30 $1.75 +1.20; MOS×3 yday $24.16 → 09:30 $24.00 -0.48; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×2 yday $8.39 → 09:30 $8.49 +0.20; RZLT×3 yday $5.04 → 09:30 $5.07 +0.09; BMEA×10 yday $1.71 → 09:30 $1.74 +0.30; NPWR×8 yday $1.81 → 09:30 $1.83 +0.16; RRC×106 yday $41.55 → 09:30 $41.44 -11.66; CRK×76 yday $14.50 → 09:30 $14.42 -6.08; SLI×416 yday $2.61 → 09:30 $2.60 -4.16; ACMR×13 yday $79.11 → 09:30 $81.65 +33.02; GGB×244 yday $4.46 → 09:30 $4.57 +26.84; MT×14 yday $74.53 → 09:30 $74.54 +0.14; MU×1 yday $938.40 → 09:30 $967.01 +28.61 |
| 2026-08-31 | -5.85 | $122.47 | MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8, RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1, BHVN×3, BZ×2, CAPR×5 | $10,974.43 | -224.86 | — | — | $122.47 | $11,040.00 | MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8, RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1, BHVN×3, BZ×2, CAPR×5 | 09:30 open · cash $122.47 (unchanged overnight, no fees) · equity $10,974.43 vs prior close $11,199.29 (-224.86) because holdings re-marked: MOS×3 yday $23.76 → 09:30 $23.75 -0.03; OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×2 yday $8.31 → 09:30 $8.29 -0.04; RZLT×3 yday $4.98 → 09:30 $4.62 -1.08; BMEA×10 yday $1.68 → 09:30 $1.71 +0.30; NPWR×8 yday $1.89 → 09:30 $1.83 -0.48; RRC×106 yday $41.64 → 09:30 $41.11 -56.18; CRK×76 yday $14.62 → 09:30 $14.56 -4.56; SLI×416 yday $2.64 → 09:30 $2.51 -54.08; ACMR×13 yday $80.49 → 09:30 $75.10 -70.07; GGB×244 yday $4.70 → 09:30 $4.55 -36.60; MT×14 yday $74.63 → 09:30 $75.07 +6.16; MU×1 yday $935.39 → 09:30 $933.01 -2.38; BHVN×3 yday $16.12 → 09:30 $15.44 -2.04; BZ×2 yday $18.00 → 09:30 $17.89 -0.22; CAPR×5 yday $10.06 → 09:30 $9.44 -3.10 |
| 2026-09-01 | -6.30 | $122.47 | MOS×3, OCUL×1, CRMD×2, RZLT×3, BMEA×10, NPWR×8, RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1, BHVN×3, BZ×2, CAPR×5 | $11,037.30 | -2.70 | — | MOS, OCUL, CRMD, RZLT, BMEA, NPWR | $264.65 | $11,056.59 | RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1, BHVN×3, BZ×2, CAPR×5 | 09:30 open · cash $122.47 (unchanged overnight, no fees) · equity $11,037.30 vs prior close $11,040.00 (-2.70) because holdings re-marked: MOS×3 yday $23.78 → 09:30 $24.00 +0.66; OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×2 yday $8.30 → 09:30 $8.26 -0.08; RZLT×3 yday $4.62 → 09:30 $4.69 +0.21; BMEA×10 yday $1.71 → 09:30 $1.65 -0.60; NPWR×8 yday $1.82 → 09:30 $1.78 -0.32; RRC×106 yday $41.78 → 09:30 $41.32 -48.76; CRK×76 yday $14.51 → 09:30 $14.31 -15.20; SLI×416 yday $2.51 → 09:30 $2.70 +79.04; ACMR×13 yday $75.02 → 09:30 $71.24 -49.14; GGB×244 yday $4.55 → 09:30 $4.61 +14.64; MT×14 yday $75.06 → 09:30 $74.31 -10.50; MU×1 yday $933.01 → 09:30 $955.79 +22.78; BHVN×3 yday $15.40 → 09:30 $15.45 +0.15; BZ×2 yday $17.90 → 09:30 $17.37 -1.06; CAPR×5 yday $9.36 → 09:30 $10.43 +5.35 |
| 2026-09-02 | -3.83 | $264.65 | RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1, BHVN×3, BZ×2, CAPR×5 | $11,167.93 | +111.34 | — | — | $264.65 | $11,159.44 | RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1, BHVN×3, BZ×2, CAPR×5 | 09:30 open · cash $264.65 (unchanged overnight, no fees) · equity $11,167.93 vs prior close $11,056.59 (+111.34) because holdings re-marked: RRC×106 yday $41.32 → 09:30 $41.94 +65.72; CRK×76 yday $14.90 → 09:30 $15.82 +69.92; SLI×416 yday $2.70 → 09:30 $2.67 -12.48; ACMR×13 yday $71.88 → 09:30 $71.44 -5.72; GGB×244 yday $4.61 → 09:30 $4.57 -9.76; MT×14 yday $73.25 → 09:30 $73.22 -0.42; MU×1 yday $940.00 → 09:30 $941.12 +1.12; BHVN×3 yday $15.45 → 09:30 $15.39 -0.18; BZ×2 yday $17.17 → 09:30 $17.29 +0.24; CAPR×5 yday $10.19 → 09:30 $10.77 +2.90 |
| 2026-09-03 | -0.90 | $264.65 | RRC×106, CRK×76, SLI×416, ACMR×13, GGB×244, MT×14, MU×1, BHVN×3, BZ×2, CAPR×5 | $11,145.12 | -14.32 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO | RRC, SLI, ACMR, GGB, MT, MU | $152.73 | $11,865.25 | CRK×76, BHVN×3, BZ×2, CAPR×5, ATRC×78, HRMY×23, CABA×299, VSTM×127, RVTY×7, GPRO×803, FRVO×53 | 09:30 open · cash $264.65 (unchanged overnight, no fees) · equity $11,145.12 vs prior close $11,159.44 (-14.32) because holdings re-marked: RRC×106 yday $42.40 → 09:30 $42.10 -31.80; CRK×76 yday $16.02 → 09:30 $15.70 -24.32; SLI×416 yday $2.49 → 09:30 $2.49 +0.00; ACMR×13 yday $70.04 → 09:30 $70.52 +6.24; GGB×244 yday $4.69 → 09:30 $4.81 +29.28; MT×14 yday $73.31 → 09:30 $73.86 +7.70; MU×1 yday $933.44 → 09:30 $930.83 -2.61; BHVN×3 yday $15.74 → 09:30 $15.97 +0.69; BZ×2 yday $17.55 → 09:30 $17.65 +0.20; CAPR×5 yday $10.01 → 09:30 $10.07 +0.30 |
| 2026-09-04 | — | $152.73 | CRK×76, BHVN×3, BZ×2, CAPR×5, ATRC×78, HRMY×23, CABA×299, VSTM×127, RVTY×7, GPRO×803, FRVO×53 | $12,000.38 | +135.13 | ASND, OSCR, NVAX, BVS, BAK | CRK, BHVN, BZ, CAPR | $53.73 | $11,507.89 | ATRC×78, HRMY×23, CABA×299, VSTM×127, RVTY×7, GPRO×803, FRVO×53, ASND×2, OSCR×7, NVAX×20, BVS×15, BAK×111 | 09:30 open · cash $152.73 (unchanged overnight, no fees) · equity $12,000.38 vs prior close $11,865.25 (+135.13) because holdings re-marked: CRK×76 yday $15.54 → 09:30 $15.45 -6.84; BHVN×3 yday $15.69 → 09:30 $15.89 +0.60; BZ×2 yday $17.30 → 09:30 $17.31 +0.02; CAPR×5 yday $9.89 → 09:30 $9.83 -0.30; ATRC×78 yday $52.59 → 09:30 $52.88 +22.62; HRMY×23 yday $42.86 → 09:30 $42.93 +1.61; CABA×299 yday $3.57 → 09:30 $3.63 +17.94; VSTM×127 yday $8.02 → 09:30 $8.03 +1.27; RVTY×7 yday $130.94 → 09:30 $132.45 +10.57; GPRO×803 yday $1.69 → 09:30 $1.78 +72.27; FRVO×53 yday $17.98 → 09:30 $18.27 +15.37 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $4000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+12.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+6.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+19.7; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.57 | ▼ 09:30 equity $10,109.78 vs yday $10,123.05 (-13.27) | 09:30 open · cash $160.57 (unchanged overnight, no fees) · equity $10,109.78 vs prior close $10,123.05 (-13.27) because holdings re-marked: BTSG×66 yday $60.23 → 09:30 $59.65 -38.28; IREN×18 yday $44.76 → 09:30 $44.09 -12.06; TPG×16 yday $54.62 → 09:30 $55.29 +10.72; TGTX×17 yday $47.94 → 09:30 $47.27 -11.39; SLS×73 yday $12.36 → 09:30 $12.40 +2.92; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×1058 yday $0.90 → 09:30 $0.93 +31.74; TNDM×36 yday $23.13 → 09:30 $22.92 -7.56 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $151.47 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-13.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $138.18 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $124.52 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $124.52 | ▼ 09:30 equity $10,380.01 vs yday $10,395.68 (-15.67) | 09:30 open · cash $124.52 (unchanged overnight, no fees) · equity $10,380.01 vs prior close $10,395.68 (-15.67) because holdings re-marked: BTSG×66 yday $61.71 → 09:30 $61.69 -1.32; IREN×18 yday $44.06 → 09:30 $45.23 +21.06; TPG×16 yday $53.03 → 09:30 $52.67 -5.76; TGTX×17 yday $48.74 → 09:30 $48.74 +0.00; SLS×73 yday $12.78 → 09:30 $12.78 +0.00; HIMS×28 yday $28.15 → 09:30 $28.14 -0.28; INO×1058 yday $1.09 → 09:30 $1.07 -21.16; TNDM×36 yday $22.72 → 09:30 $22.50 -7.92; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×14 yday $0.90 → 09:30 $0.91 +0.14; BTBT×9 yday $1.57 → 09:30 $1.52 -0.45 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $77.87 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+6.7; leftover $49.81 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.68 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-12.3; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.14 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.4; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.31 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+0.3; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.59 | — | 40% to #1, rest split; list flatten; ⚪; ret5=-11.4; leftover $10.67 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.59 | ▼ 09:30 equity $10,277.67 vs yday $10,388.13 (-110.46) | 09:30 open · cash $41.59 (unchanged overnight, no fees) · equity $10,277.67 vs prior close $10,388.13 (-110.46) because holdings re-marked: BTSG×66 yday $60.38 → 09:30 $60.00 -25.08; IREN×18 yday $44.90 → 09:30 $43.56 -24.12; TPG×16 yday $51.77 → 09:30 $51.77 +0.00; TGTX×17 yday $49.28 → 09:30 $49.28 +0.00; SLS×73 yday $13.00 → 09:30 $12.66 -24.82; HIMS×28 yday $28.61 → 09:30 $27.85 -21.28; INO×1058 yday $1.15 → 09:30 $1.14 -10.58; TNDM×36 yday $22.25 → 09:30 $22.16 -3.42; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.60 → 09:30 $1.54 -0.54; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.59 | ▲ 09:30 equity $10,504.56 vs yday $10,375.43 (+129.13) | 09:30 open · cash $41.59 (unchanged overnight, no fees) · equity $10,504.56 vs prior close $10,375.43 (+129.13) because holdings re-marked: BTSG×66 yday $59.50 → 09:30 $60.15 +42.90; IREN×18 yday $42.00 → 09:30 $41.41 -10.53; TPG×16 yday $52.02 → 09:30 $52.26 +3.84; TGTX×17 yday $50.26 → 09:30 $51.62 +23.12; SLS×73 yday $13.10 → 09:30 $13.46 +26.28; HIMS×28 yday $27.39 → 09:30 $27.55 +4.48; INO×1058 yday $1.20 → 09:30 $1.22 +21.16; TNDM×36 yday $23.73 → 09:30 $24.20 +16.92; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×14 yday $0.86 → 09:30 $0.88 +0.31; BTBT×9 yday $1.45 → 09:30 $1.42 -0.27; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $41.59 | ▼ 09:30 equity $10,599.55 vs yday $10,678.44 (-78.89) | 09:30 open · cash $41.59 (unchanged overnight, no fees) · equity $10,599.55 vs prior close $10,678.44 (-78.89) because holdings re-marked: BTSG×66 yday $59.33 → 09:30 $58.64 -45.54; IREN×18 yday $42.84 → 09:30 $42.46 -6.84; TPG×16 yday $53.18 → 09:30 $53.06 -1.92; TGTX×17 yday $51.69 → 09:30 $51.65 -0.68; SLS×73 yday $13.85 → 09:30 $13.84 -0.73; HIMS×28 yday $31.09 → 09:30 $30.66 -12.04; INO×1058 yday $1.30 → 09:30 $1.30 +0.00; TNDM×36 yday $23.46 → 09:30 $23.11 -12.60; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.40 → 09:30 $1.46 +0.50; DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 66 | $58.64 | $2.23 | $-80.98 | $3,909.60 | ▼ -80.98 after sell → book $10,597.32; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 18 | $42.46 | $2.06 | $-67.47 | $4,671.82 | ▼ -67.47 after sell → book $10,595.25; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 16 | $53.06 | $2.06 | $+34.89 | $5,518.72 | ▲ +34.89 after sell → book $10,593.20; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 17 | $51.65 | $2.06 | $+29.05 | $6,394.71 | ▲ +29.05 after sell → book $10,591.14; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 73 | $13.84 | $2.23 | $+151.78 | $7,402.79 | ▲ +151.78 after sell → book $10,588.90; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 28 | $30.66 | $2.09 | $+21.59 | $8,259.18 | ▲ +21.59 after sell → book $10,586.81; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1058 | $1.30 | $13.83 | $+492.84 | $9,620.75 | ▲ +492.84 after sell → book $10,572.98; vs 09:30 mark -13.83 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 36 | $23.11 | $2.12 | $-12.14 | $10,450.59 | ▼ -12.14 after sell → book $10,570.86; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 203 | $20.55 | $2.62 | — | $6,276.32 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $4180.24 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,455.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 43 | $20.65 | $2.12 | — | $4,565.14 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 155 | $5.77 | $2.46 | — | $3,668.34 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 45 | $19.63 | $2.12 | — | $2,782.86 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 30 | $29.63 | $2.08 | — | $1,891.88 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 511 | $1.75 | $6.59 | — | $991.04 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $121.79 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $895.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.79 | ▲ 09:30 equity $11,128.77 vs yday $10,821.22 (+307.55) | 09:30 open · cash $121.79 (unchanged overnight, no fees) · equity $11,128.77 vs prior close $10,821.22 (+307.55) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×14 yday $0.87 → 09:30 $0.87 -0.04; BTBT×9 yday $1.59 → 09:30 $1.66 +0.58; DVN×1 yday $49.30 → 09:30 $49.45 +0.15; TMC×2 yday $3.97 → 09:30 $4.10 +0.26; TGB×1 yday $8.69 → 09:30 $9.00 +0.31; DNN×3 yday $3.14 → 09:30 $3.23 +0.27; HNST×2 yday $4.96 → 09:30 $4.97 +0.02; AG×203 yday $21.19 → 09:30 $21.90 +144.13; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×43 yday $21.11 → 09:30 $21.75 +27.52; HDSN×155 yday $5.57 → 09:30 $5.67 +15.50; IAG×45 yday $20.50 → 09:30 $21.17 +30.15; KGC×30 yday $31.43 → 09:30 $32.17 +22.20; NFGC×511 yday $1.75 → 09:30 $1.79 +20.44; WPM×6 yday $150.25 → 09:30 $154.70 +26.70 | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $133.35 | ▲ +2.46 after sell → book $11,128.63; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 14 | $0.87 | $0.18 | $-1.34 | $145.31 | ▼ -1.34 after sell → book $11,128.45; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 9 | $1.66 | $0.20 | $+1.08 | $160.05 | ▲ +1.08 after sell → book $11,128.25; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $148.81 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $13.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 5 | $2.47 | $0.14 | — | $136.32 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $13.72 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 7 | $1.93 | $0.16 | — | $122.65 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $13.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 10 | $1.32 | $0.16 | — | $109.29 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $13.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.29 | ▲ 09:30 equity $11,149.50 vs yday $11,000.40 (+149.10) | 09:30 open · cash $109.29 (unchanged overnight, no fees) · equity $11,149.50 vs prior close $11,000.40 (+149.10) because holdings re-marked: DVN×1 yday $49.10 → 09:30 $48.84 -0.26; TMC×2 yday $4.79 → 09:30 $4.57 -0.44; TGB×1 yday $9.19 → 09:30 $9.26 +0.07; DNN×3 yday $3.50 → 09:30 $3.50 +0.00; HNST×2 yday $5.05 → 09:30 $5.05 +0.00; AG×203 yday $21.09 → 09:30 $21.47 +77.14; BHP×9 yday $97.03 → 09:30 $97.34 +2.79; CDE×43 yday $20.97 → 09:30 $21.26 +12.47; HDSN×155 yday $5.63 → 09:30 $5.69 +9.30; IAG×45 yday $21.14 → 09:30 $21.44 +13.50; KGC×30 yday $32.76 → 09:30 $33.21 +13.50; NFGC×511 yday $1.84 → 09:30 $1.86 +10.22; WPM×6 yday $157.78 → 09:30 $158.96 +7.08; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×5 yday $2.41 → 09:30 $2.36 -0.25; CRDL×7 yday $1.86 → 09:30 $1.87 +0.07; CYPH×10 yday $1.42 → 09:30 $1.83 +4.10 | — |
| 2026-08-24 09:30 ET | **SELL** | `DVN` | 1 | $48.84 | $0.51 | $+1.68 | $157.62 | ▲ +1.68 after sell → book $11,148.99; vs 09:30 mark -0.51 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.57 | $0.12 | $+0.84 | $166.64 | ▲ +0.84 after sell → book $11,148.87; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $175.79 | ▲ +0.60 after sell → book $11,148.76; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $186.15 | ▲ +0.54 after sell → book $11,148.62; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $196.13 | ▲ +0.25 after sell → book $11,148.50; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $196.13 | ▲ 09:30 equity $10,969.97 vs yday $10,895.06 (+74.91) | 09:30 open · cash $196.13 (unchanged overnight, no fees) · equity $10,969.97 vs prior close $10,895.06 (+74.91) because holdings re-marked: AG×203 yday $20.57 → 09:30 $20.73 +32.48; BHP×9 yday $96.66 → 09:30 $95.95 -6.39; CDE×43 yday $20.49 → 09:30 $20.85 +15.48; HDSN×155 yday $5.57 → 09:30 $5.53 -6.20; IAG×45 yday $21.36 → 09:30 $21.63 +12.15; KGC×30 yday $32.47 → 09:30 $32.76 +8.70; NFGC×511 yday $1.90 → 09:30 $1.91 +5.11; WPM×6 yday $158.00 → 09:30 $160.00 +12.00; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; AUTL×5 yday $2.38 → 09:30 $2.32 -0.30; CRDL×7 yday $1.80 → 09:30 $1.90 +0.70; CYPH×10 yday $1.64 → 09:30 $1.70 +0.60 | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 3 | $24.00 | $0.73 | — | $123.40 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.0; leftover $78.45 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $112.36 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+10.4; leftover $16.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 2 | $8.28 | $0.17 | — | $95.63 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.8; leftover $16.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 3 | $5.23 | $0.17 | — | $79.78 | — | 40% to #1, rest split; list flatten; ret5=+10.7; leftover $16.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 10 | $1.62 | $0.19 | — | $63.38 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $16.81 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 8 | $2.00 | $0.18 | — | $47.20 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $16.81 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.20 | ▲ 09:30 equity $10,917.14 vs yday $10,917.14 (+0.00) | 09:30 open · cash $47.20 (unchanged overnight, no fees) · equity $10,917.14 vs prior close $10,917.14 (+0.00) because holdings re-marked: AG×203 yday $20.68 → 09:30 $20.68 +0.00; BHP×9 yday $96.05 → 09:30 $96.05 +0.00; CDE×43 yday $20.71 → 09:30 $20.71 +0.00; HDSN×155 yday $5.49 → 09:30 $5.49 +0.00; IAG×45 yday $21.48 → 09:30 $21.48 +0.00; KGC×30 yday $32.55 → 09:30 $32.55 +0.00; NFGC×511 yday $1.90 → 09:30 $1.90 +0.00; WPM×6 yday $158.25 → 09:30 $158.25 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; AUTL×5 yday $2.34 → 09:30 $2.34 +0.00; CRDL×7 yday $1.90 → 09:30 $1.90 +0.00; CYPH×10 yday $1.64 → 09:30 $1.64 +0.00; MOS×3 yday $23.75 → 09:30 $23.75 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×2 yday $8.28 → 09:30 $8.28 +0.00; RZLT×3 yday $5.29 → 09:30 $5.29 +0.00; BMEA×10 yday $1.61 → 09:30 $1.61 +0.00; NPWR×8 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.20 | ▲ 09:30 equity $11,021.52 vs yday $10,968.41 (+53.11) | 09:30 open · cash $47.20 (unchanged overnight, no fees) · equity $11,021.52 vs prior close $10,968.41 (+53.11) because holdings re-marked: AG×203 yday $20.68 → 09:30 $20.63 -10.15; BHP×9 yday $96.05 → 09:30 $96.99 +8.46; CDE×43 yday $20.71 → 09:30 $21.00 +12.47; HDSN×155 yday $5.49 → 09:30 $5.51 +3.10; IAG×45 yday $21.48 → 09:30 $21.64 +7.20; KGC×30 yday $32.55 → 09:30 $32.90 +10.50; NFGC×511 yday $1.90 → 09:30 $2.00 +51.10; WPM×6 yday $158.25 → 09:30 $160.93 +16.08; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; AUTL×5 yday $2.34 → 09:30 $2.41 +0.35; CRDL×7 yday $1.90 → 09:30 $2.03 +0.91; CYPH×10 yday $1.64 → 09:30 $1.60 -0.40; MOS×3 yday $23.75 → 09:30 $24.84 +3.27; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×2 yday $8.28 → 09:30 $8.60 +0.64; RZLT×3 yday $5.29 → 09:30 $5.01 -0.84; BMEA×10 yday $1.61 → 09:30 $1.75 +1.40; NPWR×8 yday $2.02 → 09:30 $1.93 -0.72 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 203 | $20.63 | $2.69 | $+10.94 | $4,232.40 | ▲ +10.94 after sell → book $11,018.83; vs 09:30 mark -2.69 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 9 | $96.99 | $2.04 | $+49.77 | $5,103.28 | ▲ +49.77 after sell → book $11,016.80; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 43 | $21.00 | $2.14 | $+10.79 | $6,004.14 | ▲ +10.79 after sell → book $11,014.66; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 155 | $5.51 | $2.49 | $-45.25 | $6,855.70 | ▼ -45.25 after sell → book $11,012.17; vs 09:30 mark -2.49 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 45 | $21.64 | $2.15 | $+86.18 | $7,827.35 | ▲ +86.18 after sell → book $11,010.02; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 30 | $32.90 | $2.10 | $+93.92 | $8,812.25 | ▲ +93.92 after sell → book $11,007.92; vs 09:30 mark -2.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 511 | $2.00 | $6.69 | $+114.47 | $9,827.57 | ▲ +114.47 after sell → book $11,001.24; vs 09:30 mark -6.68 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 6 | $160.93 | $2.03 | $+94.30 | $10,791.12 | ▲ +94.30 after sell → book $10,999.21; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 106 | $40.72 | $2.31 | — | $6,472.49 | — | 40% to #1, rest split; list flatten; ret5=+1.8; leftover $4316.45 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 76 | $14.09 | $2.22 | — | $5,399.43 | — | 40% to #1, rest split; list flatten; ret5=+1.1; leftover $1079.11 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 416 | $2.59 | $5.37 | — | $4,316.63 | — | 40% to #1, rest split; list flatten; ret5=+4.2; leftover $1079.11 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 13 | $80.97 | $2.03 | — | $3,261.99 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-1.3; leftover $1079.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 244 | $4.42 | $3.15 | — | $2,180.36 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-8.6; leftover $1079.11 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 14 | $75.12 | $2.03 | — | $1,126.65 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-2.2; leftover $1079.11 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $198.91 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-0.5; leftover $1079.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $198.91 | ▲ 09:30 equity $11,162.50 vs yday $11,094.95 (+67.55) | 09:30 open · cash $198.91 (unchanged overnight, no fees) · equity $11,162.50 vs prior close $11,094.95 (+67.55) because holdings re-marked: ARCT×1 yday $15.83 → 09:30 $15.74 -0.09; AUTL×5 yday $2.33 → 09:30 $2.32 -0.05; CRDL×7 yday $2.14 → 09:30 $2.09 -0.35; CYPH×10 yday $1.63 → 09:30 $1.75 +1.20; MOS×3 yday $24.16 → 09:30 $24.00 -0.48; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×2 yday $8.39 → 09:30 $8.49 +0.20; RZLT×3 yday $5.04 → 09:30 $5.07 +0.09; BMEA×10 yday $1.71 → 09:30 $1.74 +0.30; NPWR×8 yday $1.81 → 09:30 $1.83 +0.16; RRC×106 yday $41.55 → 09:30 $41.44 -11.66; CRK×76 yday $14.50 → 09:30 $14.42 -6.08; SLI×416 yday $2.61 → 09:30 $2.60 -4.16; ACMR×13 yday $79.11 → 09:30 $81.65 +33.02; GGB×244 yday $4.46 → 09:30 $4.57 +26.84; MT×14 yday $74.53 → 09:30 $74.54 +0.14; MU×1 yday $938.40 → 09:30 $967.01 +28.61 | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.74 | $0.18 | $+4.32 | $214.47 | ▲ +4.32 after sell → book $11,162.32; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 5 | $2.32 | $0.15 | $-1.04 | $225.92 | ▼ -1.04 after sell → book $11,162.17; vs 09:30 mark -0.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 7 | $2.09 | $0.19 | $+0.78 | $240.37 | ▲ +0.78 after sell → book $11,161.99; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 10 | $1.75 | $0.23 | $+3.91 | $257.64 | ▲ +3.91 after sell → book $11,161.76; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 3 | $16.95 | $0.52 | — | $206.27 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $51.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 2 | $18.50 | $0.38 | — | $168.90 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $51.53 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 5 | $9.19 | $0.47 | — | $122.47 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $51.53 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.47 | ▼ 09:30 equity $10,974.43 vs yday $11,199.29 (-224.86) | 09:30 open · cash $122.47 (unchanged overnight, no fees) · equity $10,974.43 vs prior close $11,199.29 (-224.86) because holdings re-marked: MOS×3 yday $23.76 → 09:30 $23.75 -0.03; OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×2 yday $8.31 → 09:30 $8.29 -0.04; RZLT×3 yday $4.98 → 09:30 $4.62 -1.08; BMEA×10 yday $1.68 → 09:30 $1.71 +0.30; NPWR×8 yday $1.89 → 09:30 $1.83 -0.48; RRC×106 yday $41.64 → 09:30 $41.11 -56.18; CRK×76 yday $14.62 → 09:30 $14.56 -4.56; SLI×416 yday $2.64 → 09:30 $2.51 -54.08; ACMR×13 yday $80.49 → 09:30 $75.10 -70.07; GGB×244 yday $4.70 → 09:30 $4.55 -36.60; MT×14 yday $74.63 → 09:30 $75.07 +6.16; MU×1 yday $935.39 → 09:30 $933.01 -2.38; BHVN×3 yday $16.12 → 09:30 $15.44 -2.04; BZ×2 yday $18.00 → 09:30 $17.89 -0.22; CAPR×5 yday $10.06 → 09:30 $9.44 -3.10 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $122.47 | ▼ 09:30 equity $11,037.30 vs yday $11,040.00 (-2.70) | 09:30 open · cash $122.47 (unchanged overnight, no fees) · equity $11,037.30 vs prior close $11,040.00 (-2.70) because holdings re-marked: MOS×3 yday $23.78 → 09:30 $24.00 +0.66; OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×2 yday $8.30 → 09:30 $8.26 -0.08; RZLT×3 yday $4.62 → 09:30 $4.69 +0.21; BMEA×10 yday $1.71 → 09:30 $1.65 -0.60; NPWR×8 yday $1.82 → 09:30 $1.78 -0.32; RRC×106 yday $41.78 → 09:30 $41.32 -48.76; CRK×76 yday $14.51 → 09:30 $14.31 -15.20; SLI×416 yday $2.51 → 09:30 $2.70 +79.04; ACMR×13 yday $75.02 → 09:30 $71.24 -49.14; GGB×244 yday $4.55 → 09:30 $4.61 +14.64; MT×14 yday $75.06 → 09:30 $74.31 -10.50; MU×1 yday $933.01 → 09:30 $955.79 +22.78; BHVN×3 yday $15.40 → 09:30 $15.45 +0.15; BZ×2 yday $17.90 → 09:30 $17.37 -1.06; CAPR×5 yday $9.36 → 09:30 $10.43 +5.35 | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 3 | $24.00 | $0.75 | $-1.48 | $193.72 | ▼ -1.48 after sell → book $11,036.55; vs 09:30 mark -0.75 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $204.09 | ▼ -0.67 after sell → book $11,036.43; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 2 | $8.26 | $0.19 | $-0.40 | $220.41 | ▼ -0.40 after sell → book $11,036.23; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 3 | $4.69 | $0.17 | $-1.96 | $234.31 | ▼ -1.96 after sell → book $11,036.06; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 10 | $1.65 | $0.21 | $-0.11 | $250.60 | ▼ -0.11 after sell → book $11,035.85; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 8 | $1.78 | $0.19 | $-2.13 | $264.65 | ▼ -2.13 after sell → book $11,035.66; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $264.65 | ▲ 09:30 equity $11,167.93 vs yday $11,056.59 (+111.34) | 09:30 open · cash $264.65 (unchanged overnight, no fees) · equity $11,167.93 vs prior close $11,056.59 (+111.34) because holdings re-marked: RRC×106 yday $41.32 → 09:30 $41.94 +65.72; CRK×76 yday $14.90 → 09:30 $15.82 +69.92; SLI×416 yday $2.70 → 09:30 $2.67 -12.48; ACMR×13 yday $71.88 → 09:30 $71.44 -5.72; GGB×244 yday $4.61 → 09:30 $4.57 -9.76; MT×14 yday $73.25 → 09:30 $73.22 -0.42; MU×1 yday $940.00 → 09:30 $941.12 +1.12; BHVN×3 yday $15.45 → 09:30 $15.39 -0.18; BZ×2 yday $17.17 → 09:30 $17.29 +0.24; CAPR×5 yday $10.19 → 09:30 $10.77 +2.90 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $264.65 | ▼ 09:30 equity $11,145.12 vs yday $11,159.44 (-14.32) | 09:30 open · cash $264.65 (unchanged overnight, no fees) · equity $11,145.12 vs prior close $11,159.44 (-14.32) because holdings re-marked: RRC×106 yday $42.40 → 09:30 $42.10 -31.80; CRK×76 yday $16.02 → 09:30 $15.70 -24.32; SLI×416 yday $2.49 → 09:30 $2.49 +0.00; ACMR×13 yday $70.04 → 09:30 $70.52 +6.24; GGB×244 yday $4.69 → 09:30 $4.81 +29.28; MT×14 yday $73.31 → 09:30 $73.86 +7.70; MU×1 yday $933.44 → 09:30 $930.83 -2.61; BHVN×3 yday $15.74 → 09:30 $15.97 +0.69; BZ×2 yday $17.55 → 09:30 $17.65 +0.20; CAPR×5 yday $10.01 → 09:30 $10.07 +0.30 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 106 | $42.10 | $2.36 | $+141.61 | $4,724.89 | ▲ +141.61 after sell → book $11,142.76; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 416 | $2.49 | $5.45 | $-52.41 | $5,755.29 | ▼ -52.41 after sell → book $11,137.32; vs 09:30 mark -5.44 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ACMR` | 13 | $70.52 | $2.05 | $-139.93 | $6,670.00 | ▼ -139.93 after sell → book $11,135.27; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GGB` | 244 | $4.81 | $3.20 | $+88.81 | $7,840.44 | ▲ +88.81 after sell → book $11,132.07; vs 09:30 mark -3.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MT` | 14 | $73.86 | $2.05 | $-21.72 | $8,872.43 | ▼ -21.72 after sell → book $11,130.02; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MU` | 1 | $930.83 | $2.01 | $+1.08 | $9,801.24 | ▲ +1.08 after sell → book $11,128.00; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 78 | $49.76 | $2.22 | — | $5,917.74 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+10.6; leftover $3920.50 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 23 | $41.31 | $2.06 | — | $4,965.55 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $980.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 299 | $3.27 | $3.86 | — | $3,983.96 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+13.8; leftover $980.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 127 | $7.70 | $2.37 | — | $3,003.69 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $980.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $125.94 | $2.01 | — | $2,120.10 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $980.12 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 803 | $1.22 | $10.36 | — | $1,130.08 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $980.12 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 53 | $18.40 | $2.15 | — | $152.73 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $980.12 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.73 | ▲ 09:30 equity $12,000.38 vs yday $11,865.25 (+135.13) | 09:30 open · cash $152.73 (unchanged overnight, no fees) · equity $12,000.38 vs prior close $11,865.25 (+135.13) because holdings re-marked: CRK×76 yday $15.54 → 09:30 $15.45 -6.84; BHVN×3 yday $15.69 → 09:30 $15.89 +0.60; BZ×2 yday $17.30 → 09:30 $17.31 +0.02; CAPR×5 yday $9.89 → 09:30 $9.83 -0.30; ATRC×78 yday $52.59 → 09:30 $52.88 +22.62; HRMY×23 yday $42.86 → 09:30 $42.93 +1.61; CABA×299 yday $3.57 → 09:30 $3.63 +17.94; VSTM×127 yday $8.02 → 09:30 $8.03 +1.27; RVTY×7 yday $130.94 → 09:30 $132.45 +10.57; GPRO×803 yday $1.69 → 09:30 $1.78 +72.27; FRVO×53 yday $17.98 → 09:30 $18.27 +15.37 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 76 | $15.45 | $2.24 | $+98.90 | $1,324.69 | ▲ +98.90 after sell → book $11,998.14; vs 09:30 mark -2.24 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 3 | $15.89 | $0.51 | $-4.20 | $1,371.86 | ▼ -4.20 after sell → book $11,997.64; vs 09:30 mark -0.50 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 2 | $17.31 | $0.37 | $-3.13 | $1,406.11 | ▼ -3.13 after sell → book $11,997.27; vs 09:30 mark -0.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 5 | $9.83 | $0.53 | $+2.20 | $1,454.73 | ▲ +2.20 after sell → book $11,996.74; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 2 | $266.94 | $2.00 | — | $918.85 | — | 40% to #1, rest split; list flatten; ret5=+1.9; leftover $581.89 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 7 | $30.65 | $2.01 | — | $702.29 | — | 40% to #1, rest split; list flatten; 🔵; ret5=-2.2; leftover $218.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 20 | $10.41 | $2.05 | — | $492.04 | — | 40% to #1, rest split; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $218.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 15 | $14.50 | $2.04 | — | $272.51 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.8; leftover $218.21 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 111 | $1.95 | $2.32 | — | $53.73 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $218.21 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 64.23 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
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
| 2026-08-17 | `EOG` | cash | leftover split 10.67 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.67 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.67 < 1 share @ 90.54 |
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
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `DVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `DVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 64.02 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 13.72 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 13.72 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 13.72 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `INSP` | cash | leftover split 16.81 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 16.81 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
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
| 2026-08-28 | `ANF` | cash | leftover split 103.06 < 1 share @ 144.70 |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
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
| `ATRC` | 78 | 2026-09-03 @ $49.76 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+10.6; leftover $3920.50 |
| `HRMY` | 23 | 2026-09-03 @ $41.31 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $980.12 |
| `CABA` | 299 | 2026-09-03 @ $3.27 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+13.8; leftover $980.12 |
| `VSTM` | 127 | 2026-09-03 @ $7.70 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $980.12 |
| `RVTY` | 7 | 2026-09-03 @ $125.94 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $980.12 |
| `GPRO` | 803 | 2026-09-03 @ $1.22 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $980.12 |
| `FRVO` | 53 | 2026-09-03 @ $18.40 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $980.12 |
| `ASND` | 2 | 2026-09-04 @ $266.94 | 40% to #1, rest split; list flatten; ret5=+1.9; leftover $581.89 |
| `OSCR` | 7 | 2026-09-04 @ $30.65 | 40% to #1, rest split; list flatten; 🔵; ret5=-2.2; leftover $218.21 |
| `NVAX` | 20 | 2026-09-04 @ $10.41 | 40% to #1, rest split; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $218.21 |
| `BVS` | 15 | 2026-09-04 @ $14.50 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.8; leftover $218.21 |
| `BAK` | 111 | 2026-09-04 @ $1.95 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $218.21 |
