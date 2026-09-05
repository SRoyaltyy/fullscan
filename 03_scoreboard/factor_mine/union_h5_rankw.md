# Factor mine action — `union_h5_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **+12.88%** ($11,288) · signal-only (no cash/fees) was +58.01%. Starts YES **16/17**. Fills 94 · skips 233 · realized $+916.73.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $179.45.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $128.05 | $10,117.03 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $128.05 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | $10,103.42 | -13.61 | MARA, LDI, BTBT | — | $109.27 | $10,260.71 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2 | 09:30 open · cash $128.05 (unchanged overnight, no fees) · equity $10,103.42 vs prior close $10,117.03 (-13.61) because holdings re-marked: BTSG×37 yday $60.23 → 09:30 $59.65 -21.46; IREN×42 yday $44.76 → 09:30 $44.09 -28.14; TPG×32 yday $54.62 → 09:30 $55.29 +21.44; TGTX×27 yday $47.94 → 09:30 $47.27 -18.09; SLS×94 yday $12.36 → 09:30 $12.40 +3.76; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×685 yday $0.90 → 09:30 $0.93 +20.55; TNDM×11 yday $23.13 → 09:30 $22.92 -2.31 |
| 2026-08-17 | +2.25 | $109.27 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2 | $10,281.18 | +20.47 | TMC, TGB, DNN | — | $85.16 | $10,290.17 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | 09:30 open · cash $109.27 (unchanged overnight, no fees) · equity $10,281.18 vs prior close $10,260.71 (+20.47) because holdings re-marked: BTSG×37 yday $61.71 → 09:30 $61.69 -0.74; IREN×42 yday $44.06 → 09:30 $45.23 +49.14; TPG×32 yday $53.03 → 09:30 $52.67 -11.52; TGTX×27 yday $48.74 → 09:30 $48.74 +0.00; SLS×94 yday $12.78 → 09:30 $12.78 +0.00; HIMS×28 yday $28.15 → 09:30 $28.14 -0.28; INO×685 yday $1.09 → 09:30 $1.07 -13.70; TNDM×11 yday $22.72 → 09:30 $22.50 -2.42; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×7 yday $0.90 → 09:30 $0.91 +0.07; BTBT×2 yday $1.57 → 09:30 $1.52 -0.10 |
| 2026-08-18 | -6.20 | $85.16 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | $10,157.73 | -132.44 | — | — | $85.16 | $10,194.81 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | 09:30 open · cash $85.16 (unchanged overnight, no fees) · equity $10,157.73 vs prior close $10,290.17 (-132.44) because holdings re-marked: BTSG×37 yday $60.38 → 09:30 $60.00 -14.06; IREN×42 yday $44.90 → 09:30 $43.56 -56.28; TPG×32 yday $51.77 → 09:30 $51.77 +0.00; TGTX×27 yday $49.28 → 09:30 $49.28 +0.00; SLS×94 yday $13.00 → 09:30 $12.66 -31.96; HIMS×28 yday $28.61 → 09:30 $27.85 -21.28; INO×685 yday $1.15 → 09:30 $1.14 -6.85; TNDM×11 yday $22.25 → 09:30 $22.16 -1.04; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×2 yday $1.60 → 09:30 $1.54 -0.12; TMC×3 yday $3.77 → 09:30 $3.72 -0.15; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×1 yday $3.19 → 09:30 $3.11 -0.08 |
| 2026-08-19 | -7.20 | $85.16 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | $10,296.33 | +101.52 | — | — | $85.16 | $10,540.20 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | 09:30 open · cash $85.16 (unchanged overnight, no fees) · equity $10,296.33 vs prior close $10,194.81 (+101.52) because holdings re-marked: BTSG×37 yday $59.50 → 09:30 $60.15 +24.05; IREN×42 yday $42.00 → 09:30 $41.41 -24.57; TPG×32 yday $52.02 → 09:30 $52.26 +7.68; TGTX×27 yday $50.26 → 09:30 $51.62 +36.72; SLS×94 yday $13.10 → 09:30 $13.46 +33.84; HIMS×28 yday $27.39 → 09:30 $27.55 +4.48; INO×685 yday $1.20 → 09:30 $1.22 +13.70; TNDM×11 yday $23.73 → 09:30 $24.20 +5.17; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×7 yday $0.86 → 09:30 $0.88 +0.15; BTBT×2 yday $1.45 → 09:30 $1.42 -0.06; TMC×3 yday $3.92 → 09:30 $3.93 +0.03; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 |
| 2026-08-20 | +1.12 | $85.16 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | $10,477.31 | -62.89 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $45.26 | $10,666.78 | MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1, AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2 | 09:30 open · cash $85.16 (unchanged overnight, no fees) · equity $10,477.31 vs prior close $10,540.20 (-62.89) because holdings re-marked: BTSG×37 yday $59.33 → 09:30 $58.64 -25.53; IREN×42 yday $42.84 → 09:30 $42.46 -15.96; TPG×32 yday $53.18 → 09:30 $53.06 -3.84; TGTX×27 yday $51.69 → 09:30 $51.65 -1.08; SLS×94 yday $13.85 → 09:30 $13.84 -0.94; HIMS×28 yday $31.09 → 09:30 $30.66 -12.04; INO×685 yday $1.30 → 09:30 $1.30 +0.00; TNDM×11 yday $23.46 → 09:30 $23.11 -3.85; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×2 yday $1.40 → 09:30 $1.46 +0.11; TMC×3 yday $3.97 → 09:30 $3.92 -0.15; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 |
| 2026-08-21 | +3.25 | $45.26 | MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1, AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2 | $10,954.91 | +288.13 | AUTL, CRDL, CYPH | MARA, LDI, BTBT | $55.82 | $10,857.48 | TMC×3, TGB×1, DNN×1, AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1 | 09:30 open · cash $45.26 (unchanged overnight, no fees) · equity $10,954.91 vs prior close $10,666.78 (+288.13) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×7 yday $0.87 → 09:30 $0.87 -0.02; BTBT×2 yday $1.59 → 09:30 $1.66 +0.13; TMC×3 yday $3.97 → 09:30 $4.10 +0.39; TGB×1 yday $8.69 → 09:30 $9.00 +0.31; DNN×1 yday $3.14 → 09:30 $3.23 +0.09; AG×112 yday $21.19 → 09:30 $21.90 +79.52; BHP×22 yday $93.63 → 09:30 $95.72 +45.98; CDE×84 yday $21.11 → 09:30 $21.75 +53.76; HDSN×250 yday $5.57 → 09:30 $5.67 +25.00; IAG×58 yday $20.50 → 09:30 $21.17 +38.86; KGC×29 yday $31.43 → 09:30 $32.17 +21.46; NFGC×330 yday $1.75 → 09:30 $1.79 +13.20; WPM×2 yday $150.25 → 09:30 $154.70 +8.90 |
| 2026-08-24 | -5.17 | $55.82 | TMC×3, TGB×1, DNN×1, AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1 | $10,985.37 | +127.89 | — | TMC, TGB, DNN | $81.95 | $10,759.48 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1 | 09:30 open · cash $55.82 (unchanged overnight, no fees) · equity $10,985.37 vs prior close $10,857.48 (+127.89) because holdings re-marked: TMC×3 yday $4.79 → 09:30 $4.57 -0.66; TGB×1 yday $9.19 → 09:30 $9.26 +0.07; DNN×1 yday $3.50 → 09:30 $3.50 +0.00; AG×112 yday $21.09 → 09:30 $21.47 +42.56; BHP×22 yday $97.03 → 09:30 $97.34 +6.82; CDE×84 yday $20.97 → 09:30 $21.26 +24.36; HDSN×250 yday $5.63 → 09:30 $5.69 +15.00; IAG×58 yday $21.14 → 09:30 $21.44 +17.40; KGC×29 yday $32.76 → 09:30 $33.21 +13.05; NFGC×330 yday $1.84 → 09:30 $1.86 +6.60; WPM×2 yday $157.78 → 09:30 $158.96 +2.36; AUTL×2 yday $2.41 → 09:30 $2.36 -0.10; CRDL×2 yday $1.86 → 09:30 $1.87 +0.02; CYPH×1 yday $1.42 → 09:30 $1.83 +0.41 |
| 2026-08-25 | +1.80 | $81.95 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1 | $10,813.53 | +54.05 | OCUL, CRMD, RZLT, BMEA, NPWR | — | $51.96 | $10,766.50 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1 | 09:30 open · cash $81.95 (unchanged overnight, no fees) · equity $10,813.53 vs prior close $10,759.48 (+54.05) because holdings re-marked: AG×112 yday $20.57 → 09:30 $20.73 +17.92; BHP×22 yday $96.66 → 09:30 $95.95 -15.62; CDE×84 yday $20.49 → 09:30 $20.85 +30.24; HDSN×250 yday $5.57 → 09:30 $5.53 -10.00; IAG×58 yday $21.36 → 09:30 $21.63 +15.66; KGC×29 yday $32.47 → 09:30 $32.76 +8.41; NFGC×330 yday $1.90 → 09:30 $1.91 +3.30; WPM×2 yday $158.00 → 09:30 $160.00 +4.00; AUTL×2 yday $2.38 → 09:30 $2.32 -0.12; CRDL×2 yday $1.80 → 09:30 $1.90 +0.20; CYPH×1 yday $1.64 → 09:30 $1.70 +0.06 |
| 2026-08-26 | +2.02 | $51.96 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1 | $10,766.50 | +0.00 | — | — | $51.96 | $10,813.21 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1 | 09:30 open · cash $51.96 (unchanged overnight, no fees) · equity $10,766.50 vs prior close $10,766.50 (+0.00) because holdings re-marked: AG×112 yday $20.68 → 09:30 $20.68 +0.00; BHP×22 yday $96.05 → 09:30 $96.05 +0.00; CDE×84 yday $20.71 → 09:30 $20.71 +0.00; HDSN×250 yday $5.49 → 09:30 $5.49 +0.00; IAG×58 yday $21.48 → 09:30 $21.48 +0.00; KGC×29 yday $32.55 → 09:30 $32.55 +0.00; NFGC×330 yday $1.90 → 09:30 $1.90 +0.00; WPM×2 yday $158.25 → 09:30 $158.25 +0.00; AUTL×2 yday $2.34 → 09:30 $2.34 +0.00; CRDL×2 yday $1.90 → 09:30 $1.90 +0.00; CYPH×1 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×1 yday $8.28 → 09:30 $8.28 +0.00; RZLT×1 yday $5.29 → 09:30 $5.29 +0.00; BMEA×2 yday $1.61 → 09:30 $1.61 +0.00; NPWR×1 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $51.96 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1 | $10,869.19 | +55.98 | RRC, CRK, MOS, SLI, ACMR, GGB, MT | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $481.39 | $10,877.13 | AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7 | 09:30 open · cash $51.96 (unchanged overnight, no fees) · equity $10,869.19 vs prior close $10,813.21 (+55.98) because holdings re-marked: AG×112 yday $20.68 → 09:30 $20.63 -5.60; BHP×22 yday $96.05 → 09:30 $96.99 +20.68; CDE×84 yday $20.71 → 09:30 $21.00 +24.36; HDSN×250 yday $5.49 → 09:30 $5.51 +5.00; IAG×58 yday $21.48 → 09:30 $21.64 +9.28; KGC×29 yday $32.55 → 09:30 $32.90 +10.15; NFGC×330 yday $1.90 → 09:30 $2.00 +33.00; WPM×2 yday $158.25 → 09:30 $160.93 +5.36; AUTL×2 yday $2.34 → 09:30 $2.41 +0.14; CRDL×2 yday $1.90 → 09:30 $2.03 +0.26; CYPH×1 yday $1.64 → 09:30 $1.60 -0.04; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×1 yday $8.28 → 09:30 $8.60 +0.32; RZLT×1 yday $5.29 → 09:30 $5.01 -0.28; BMEA×2 yday $1.61 → 09:30 $1.75 +0.28; NPWR×1 yday $2.02 → 09:30 $1.93 -0.09 |
| 2026-08-28 | +0.75 | $481.39 | AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7 | $10,899.55 | +22.42 | ANF, BHVN, BZ, CAPR | AUTL, CRDL, CYPH | $68.79 | $10,949.31 | OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | 09:30 open · cash $481.39 (unchanged overnight, no fees) · equity $10,899.55 vs prior close $10,877.13 (+22.42) because holdings re-marked: AUTL×2 yday $2.33 → 09:30 $2.32 -0.02; CRDL×2 yday $2.14 → 09:30 $2.09 -0.10; CYPH×1 yday $1.63 → 09:30 $1.75 +0.12; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; RZLT×1 yday $5.04 → 09:30 $5.07 +0.03; BMEA×2 yday $1.71 → 09:30 $1.74 +0.06; NPWR×1 yday $1.81 → 09:30 $1.83 +0.02; RRC×58 yday $41.55 → 09:30 $41.44 -6.38; CRK×149 yday $14.50 → 09:30 $14.42 -11.92; MOS×72 yday $24.16 → 09:30 $24.00 -11.52; SLI×579 yday $2.61 → 09:30 $2.60 -5.79; ACMR×14 yday $79.11 → 09:30 $81.65 +35.56; GGB×203 yday $4.46 → 09:30 $4.57 +22.33; MT×7 yday $74.53 → 09:30 $74.54 +0.07 |
| 2026-08-31 | -5.85 | $68.79 | OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | $10,723.80 | -225.51 | — | — | $68.79 | $10,756.12 | OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | 09:30 open · cash $68.79 (unchanged overnight, no fees) · equity $10,723.80 vs prior close $10,949.31 (-225.51) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×1 yday $8.31 → 09:30 $8.29 -0.02; RZLT×1 yday $4.98 → 09:30 $4.62 -0.36; BMEA×2 yday $1.68 → 09:30 $1.71 +0.06; NPWR×1 yday $1.89 → 09:30 $1.83 -0.06; RRC×58 yday $41.64 → 09:30 $41.11 -30.74; CRK×149 yday $14.62 → 09:30 $14.56 -8.94; MOS×72 yday $23.76 → 09:30 $23.75 -0.72; SLI×579 yday $2.64 → 09:30 $2.51 -75.27; ACMR×14 yday $80.49 → 09:30 $75.10 -75.46; GGB×203 yday $4.70 → 09:30 $4.55 -30.45; MT×7 yday $74.63 → 09:30 $75.07 +3.08; ANF×1 yday $145.75 → 09:30 $148.67 +2.92; BHVN×8 yday $16.12 → 09:30 $15.44 -5.44; BZ×5 yday $18.00 → 09:30 $17.89 -0.55; CAPR×5 yday $10.06 → 09:30 $9.44 -3.10 |
| 2026-09-01 | -6.30 | $68.79 | OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | $10,775.79 | +19.67 | — | OCUL, CRMD, RZLT, BMEA, NPWR | $96.90 | $10,881.16 | RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | 09:30 open · cash $68.79 (unchanged overnight, no fees) · equity $10,775.79 vs prior close $10,756.12 (+19.67) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; RZLT×1 yday $4.62 → 09:30 $4.69 +0.07; BMEA×2 yday $1.71 → 09:30 $1.65 -0.12; NPWR×1 yday $1.82 → 09:30 $1.78 -0.04; RRC×58 yday $41.78 → 09:30 $41.32 -26.68; CRK×149 yday $14.51 → 09:30 $14.31 -29.80; MOS×72 yday $23.78 → 09:30 $24.00 +15.84; SLI×579 yday $2.51 → 09:30 $2.70 +110.01; ACMR×14 yday $75.02 → 09:30 $71.24 -52.92; GGB×203 yday $4.55 → 09:30 $4.61 +12.18; MT×7 yday $75.06 → 09:30 $74.31 -5.25; ANF×1 yday $149.28 → 09:30 $142.47 -6.81; BHVN×8 yday $15.40 → 09:30 $15.45 +0.40; BZ×5 yday $17.90 → 09:30 $17.37 -2.65; CAPR×5 yday $9.36 → 09:30 $10.43 +5.35 |
| 2026-09-02 | -3.83 | $96.90 | RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | $11,002.04 | +120.88 | — | — | $96.90 | $11,019.15 | RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | 09:30 open · cash $96.90 (unchanged overnight, no fees) · equity $11,002.04 vs prior close $10,881.16 (+120.88) because holdings re-marked: RRC×58 yday $41.32 → 09:30 $41.94 +35.96; CRK×149 yday $14.90 → 09:30 $15.82 +137.08; MOS×72 yday $24.25 → 09:30 $23.94 -22.32; SLI×579 yday $2.70 → 09:30 $2.67 -17.37; ACMR×14 yday $71.88 → 09:30 $71.44 -6.16; GGB×203 yday $4.61 → 09:30 $4.57 -8.12; MT×7 yday $73.25 → 09:30 $73.22 -0.21; ANF×1 yday $143.00 → 09:30 $142.00 -1.00; BHVN×8 yday $15.45 → 09:30 $15.39 -0.48; BZ×5 yday $17.17 → 09:30 $17.29 +0.60; CAPR×5 yday $10.19 → 09:30 $10.77 +2.90 |
| 2026-09-03 | -0.90 | $96.90 | RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | $10,984.85 | -34.30 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO | RRC, MOS, SLI, ACMR, GGB, MT | $169.09 | $11,525.56 | CRK×149, ANF×1, BHVN×8, BZ×5, CAPR×5, ATRC×41, HRMY×42, CABA×448, VSTM×152, RVTY×6, GPRO×481, FRVO×15 | 09:30 open · cash $96.90 (unchanged overnight, no fees) · equity $10,984.85 vs prior close $11,019.15 (-34.30) because holdings re-marked: RRC×58 yday $42.40 → 09:30 $42.10 -17.40; CRK×149 yday $16.02 → 09:30 $15.70 -47.68; MOS×72 yday $24.78 → 09:30 $24.70 -5.76; SLI×579 yday $2.49 → 09:30 $2.49 +0.00; ACMR×14 yday $70.04 → 09:30 $70.52 +6.72; GGB×203 yday $4.69 → 09:30 $4.81 +24.36; MT×7 yday $73.31 → 09:30 $73.86 +3.85; ANF×1 yday $140.68 → 09:30 $139.65 -1.03; BHVN×8 yday $15.74 → 09:30 $15.97 +1.84; BZ×5 yday $17.55 → 09:30 $17.65 +0.50; CAPR×5 yday $10.01 → 09:30 $10.07 +0.30 |
| 2026-09-04 | — | $169.09 | CRK×149, ANF×1, BHVN×8, BZ×5, CAPR×5, ATRC×41, HRMY×42, CABA×448, VSTM×152, RVTY×6, GPRO×481, FRVO×15 | $11,614.53 | +88.97 | ASND, OSCR, NVAX, BVS, BAK | CRK, ANF, BHVN, BZ, CAPR | $179.45 | $11,287.56 | ATRC×41, HRMY×42, CABA×448, VSTM×152, RVTY×6, GPRO×481, FRVO×15, ASND×3, OSCR×24, NVAX×55, BVS×26, BAK×97 | 09:30 open · cash $169.09 (unchanged overnight, no fees) · equity $11,614.53 vs prior close $11,525.56 (+88.97) because holdings re-marked: CRK×149 yday $15.54 → 09:30 $15.45 -13.41; ANF×1 yday $136.60 → 09:30 $137.70 +1.10; BHVN×8 yday $15.69 → 09:30 $15.89 +1.60; BZ×5 yday $17.30 → 09:30 $17.31 +0.05; CAPR×5 yday $9.89 → 09:30 $9.83 -0.30; ATRC×41 yday $52.59 → 09:30 $52.88 +11.89; HRMY×42 yday $42.86 → 09:30 $42.93 +2.94; CABA×448 yday $3.57 → 09:30 $3.63 +26.88; VSTM×152 yday $8.02 → 09:30 $8.03 +1.52; RVTY×6 yday $130.94 → 09:30 $132.45 +9.06; GPRO×481 yday $1.69 → 09:30 $1.78 +43.29; FRVO×15 yday $17.98 → 09:30 $18.27 +4.35 |

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
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▲ 09:30 equity $10,296.33 vs yday $10,194.81 (+101.52) | 09:30 open · cash $85.16 (unchanged overnight, no fees) · equity $10,296.33 vs prior close $10,194.81 (+101.52) because holdings re-marked: BTSG×37 yday $59.50 → 09:30 $60.15 +24.05; IREN×42 yday $42.00 → 09:30 $41.41 -24.57; TPG×32 yday $52.02 → 09:30 $52.26 +7.68; TGTX×27 yday $50.26 → 09:30 $51.62 +36.72; SLS×94 yday $13.10 → 09:30 $13.46 +33.84; HIMS×28 yday $27.39 → 09:30 $27.55 +4.48; INO×685 yday $1.20 → 09:30 $1.22 +13.70; TNDM×11 yday $23.73 → 09:30 $24.20 +5.17; MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×7 yday $0.86 → 09:30 $0.88 +0.15; BTBT×2 yday $1.45 → 09:30 $1.42 -0.06; TMC×3 yday $3.92 → 09:30 $3.93 +0.03; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×1 yday $3.15 → 09:30 $3.19 +0.04 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.16 | ▼ 09:30 equity $10,477.31 vs yday $10,540.20 (-62.89) | 09:30 open · cash $85.16 (unchanged overnight, no fees) · equity $10,477.31 vs prior close $10,540.20 (-62.89) because holdings re-marked: BTSG×37 yday $59.33 → 09:30 $58.64 -25.53; IREN×42 yday $42.84 → 09:30 $42.46 -15.96; TPG×32 yday $53.18 → 09:30 $53.06 -3.84; TGTX×27 yday $51.69 → 09:30 $51.65 -1.08; SLS×94 yday $13.85 → 09:30 $13.84 -0.94; HIMS×28 yday $31.09 → 09:30 $30.66 -12.04; INO×685 yday $1.30 → 09:30 $1.30 +0.00; TNDM×11 yday $23.46 → 09:30 $23.11 -3.85; MARA×1 yday $9.65 → 09:30 $10.21 +0.56; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×2 yday $1.40 → 09:30 $1.46 +0.11; TMC×3 yday $3.97 → 09:30 $3.92 -0.15; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×1 yday $3.22 → 09:30 $3.20 -0.02 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 37 | $58.64 | $2.13 | $-47.15 | $2,252.71 | ▼ -47.15 after sell → book $10,475.18; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 42 | $42.46 | $2.14 | $-152.10 | $4,033.89 | ▼ -152.10 after sell → book $10,473.04; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 32 | $53.06 | $2.11 | $+73.78 | $5,729.70 | ▲ +73.78 after sell → book $10,470.93; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 27 | $51.65 | $2.09 | $+48.49 | $7,122.16 | ▲ +48.49 after sell → book $10,468.84; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $8,420.82 | ▲ +196.59 after sell → book $10,466.54; vs 09:30 mark -2.30 | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 28 | $30.66 | $2.09 | $+21.59 | $9,277.21 | ▲ +21.59 after sell → book $10,464.45; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 685 | $1.30 | $8.96 | $+319.09 | $10,158.75 | ▲ +319.09 after sell → book $10,455.49; vs 09:30 mark -8.96 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 11 | $23.11 | $2.04 | $-6.49 | $10,410.92 | ▼ -6.49 after sell → book $10,453.44; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 112 | $20.55 | $2.33 | — | $8,106.99 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2313.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 22 | $91.01 | $2.06 | — | $6,102.72 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2024.35 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 84 | $20.65 | $2.24 | — | $4,365.87 | — | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1735.15 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 250 | $5.77 | $3.23 | — | $2,920.15 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1445.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 58 | $19.63 | $2.16 | — | $1,779.44 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1156.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $918.10 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $867.58 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 330 | $1.75 | $4.26 | — | $336.34 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $578.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 2 | $144.54 | $2.00 | — | $45.26 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $289.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $45.26 | ▲ 09:30 equity $10,954.91 vs yday $10,666.78 (+288.13) | 09:30 open · cash $45.26 (unchanged overnight, no fees) · equity $10,954.91 vs prior close $10,666.78 (+288.13) because holdings re-marked: MARA×1 yday $11.15 → 09:30 $11.70 +0.55; LDI×7 yday $0.87 → 09:30 $0.87 -0.02; BTBT×2 yday $1.59 → 09:30 $1.66 +0.13; TMC×3 yday $3.97 → 09:30 $4.10 +0.39; TGB×1 yday $8.69 → 09:30 $9.00 +0.31; DNN×1 yday $3.14 → 09:30 $3.23 +0.09; AG×112 yday $21.19 → 09:30 $21.90 +79.52; BHP×22 yday $93.63 → 09:30 $95.72 +45.98; CDE×84 yday $21.11 → 09:30 $21.75 +53.76; HDSN×250 yday $5.57 → 09:30 $5.67 +25.00; IAG×58 yday $20.50 → 09:30 $21.17 +38.86; KGC×29 yday $31.43 → 09:30 $32.17 +21.46; NFGC×330 yday $1.75 → 09:30 $1.79 +13.20; WPM×2 yday $150.25 → 09:30 $154.70 +8.90 | — |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $56.82 | ▲ +2.46 after sell → book $10,954.77; vs 09:30 mark -0.14 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 7 | $0.87 | $0.10 | $-0.68 | $62.79 | ▼ -0.68 after sell → book $10,954.67; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 2 | $1.66 | $0.06 | $+0.22 | $66.05 | ▲ +0.22 after sell → book $10,954.61; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 2 | $2.47 | $0.06 | — | $61.06 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $7.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 2 | $1.93 | $0.04 | — | $57.15 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $5.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1 | $1.32 | $0.02 | — | $55.82 | — | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.82 | ▲ 09:30 equity $10,985.37 vs yday $10,857.48 (+127.89) | 09:30 open · cash $55.82 (unchanged overnight, no fees) · equity $10,985.37 vs prior close $10,857.48 (+127.89) because holdings re-marked: TMC×3 yday $4.79 → 09:30 $4.57 -0.66; TGB×1 yday $9.19 → 09:30 $9.26 +0.07; DNN×1 yday $3.50 → 09:30 $3.50 +0.00; AG×112 yday $21.09 → 09:30 $21.47 +42.56; BHP×22 yday $97.03 → 09:30 $97.34 +6.82; CDE×84 yday $20.97 → 09:30 $21.26 +24.36; HDSN×250 yday $5.63 → 09:30 $5.69 +15.00; IAG×58 yday $21.14 → 09:30 $21.44 +17.40; KGC×29 yday $32.76 → 09:30 $33.21 +13.05; NFGC×330 yday $1.84 → 09:30 $1.86 +6.60; WPM×2 yday $157.78 → 09:30 $158.96 +2.36; AUTL×2 yday $2.41 → 09:30 $2.36 -0.10; CRDL×2 yday $1.86 → 09:30 $1.87 +0.02; CYPH×1 yday $1.42 → 09:30 $1.83 +0.41 | — |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 3 | $4.57 | $0.17 | $+1.26 | $69.36 | ▲ +1.26 after sell → book $10,985.20; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $78.50 | ▲ +0.60 after sell → book $10,985.08; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 1 | $3.50 | $0.06 | $+0.17 | $81.95 | ▲ +0.17 after sell → book $10,985.03; vs 09:30 mark -0.05 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.95 | ▲ 09:30 equity $10,813.53 vs yday $10,759.48 (+54.05) | 09:30 open · cash $81.95 (unchanged overnight, no fees) · equity $10,813.53 vs prior close $10,759.48 (+54.05) because holdings re-marked: AG×112 yday $20.57 → 09:30 $20.73 +17.92; BHP×22 yday $96.66 → 09:30 $95.95 -15.62; CDE×84 yday $20.49 → 09:30 $20.85 +30.24; HDSN×250 yday $5.57 → 09:30 $5.53 -10.00; IAG×58 yday $21.36 → 09:30 $21.63 +15.66; KGC×29 yday $32.47 → 09:30 $32.76 +8.41; NFGC×330 yday $1.90 → 09:30 $1.91 +3.30; WPM×2 yday $158.00 → 09:30 $160.00 +4.00; AUTL×2 yday $2.38 → 09:30 $2.32 -0.12; CRDL×2 yday $1.80 → 09:30 $1.90 +0.20; CYPH×1 yday $1.64 → 09:30 $1.70 +0.06 | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $70.91 | — | rank-weighted leftover; list flatten; 🔵; ret5=+10.4; leftover $15.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.28 | $0.09 | — | $62.55 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $11.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 1 | $5.23 | $0.06 | — | $57.26 | — | rank-weighted leftover; list flatten; ret5=+10.7; leftover $9.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 2 | $1.62 | $0.04 | — | $53.98 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $4.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 1 | $2.00 | $0.02 | — | $51.96 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $2.28 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.96 | ▲ 09:30 equity $10,766.50 vs yday $10,766.50 (+0.00) | 09:30 open · cash $51.96 (unchanged overnight, no fees) · equity $10,766.50 vs prior close $10,766.50 (+0.00) because holdings re-marked: AG×112 yday $20.68 → 09:30 $20.68 +0.00; BHP×22 yday $96.05 → 09:30 $96.05 +0.00; CDE×84 yday $20.71 → 09:30 $20.71 +0.00; HDSN×250 yday $5.49 → 09:30 $5.49 +0.00; IAG×58 yday $21.48 → 09:30 $21.48 +0.00; KGC×29 yday $32.55 → 09:30 $32.55 +0.00; NFGC×330 yday $1.90 → 09:30 $1.90 +0.00; WPM×2 yday $158.25 → 09:30 $158.25 +0.00; AUTL×2 yday $2.34 → 09:30 $2.34 +0.00; CRDL×2 yday $1.90 → 09:30 $1.90 +0.00; CYPH×1 yday $1.64 → 09:30 $1.64 +0.00; OCUL×1 yday $10.92 → 09:30 $10.92 +0.00; CRMD×1 yday $8.28 → 09:30 $8.28 +0.00; RZLT×1 yday $5.29 → 09:30 $5.29 +0.00; BMEA×2 yday $1.61 → 09:30 $1.61 +0.00; NPWR×1 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $51.96 | ▲ 09:30 equity $10,869.19 vs yday $10,813.21 (+55.98) | 09:30 open · cash $51.96 (unchanged overnight, no fees) · equity $10,869.19 vs prior close $10,813.21 (+55.98) because holdings re-marked: AG×112 yday $20.68 → 09:30 $20.63 -5.60; BHP×22 yday $96.05 → 09:30 $96.99 +20.68; CDE×84 yday $20.71 → 09:30 $21.00 +24.36; HDSN×250 yday $5.49 → 09:30 $5.51 +5.00; IAG×58 yday $21.48 → 09:30 $21.64 +9.28; KGC×29 yday $32.55 → 09:30 $32.90 +10.15; NFGC×330 yday $1.90 → 09:30 $2.00 +33.00; WPM×2 yday $158.25 → 09:30 $160.93 +5.36; AUTL×2 yday $2.34 → 09:30 $2.41 +0.14; CRDL×2 yday $1.90 → 09:30 $2.03 +0.26; CYPH×1 yday $1.64 → 09:30 $1.60 -0.04; OCUL×1 yday $10.92 → 09:30 $10.79 -0.13; CRMD×1 yday $8.28 → 09:30 $8.60 +0.32; RZLT×1 yday $5.29 → 09:30 $5.01 -0.28; BMEA×2 yday $1.61 → 09:30 $1.75 +0.28; NPWR×1 yday $2.02 → 09:30 $1.93 -0.09 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 112 | $20.63 | $2.36 | $+4.27 | $2,360.16 | ▲ +4.27 after sell → book $10,866.83; vs 09:30 mark -2.36 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 22 | $96.99 | $2.08 | $+127.42 | $4,491.86 | ▲ +127.42 after sell → book $10,864.75; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 84 | $21.00 | $2.27 | $+24.89 | $6,253.59 | ▲ +24.89 after sell → book $10,862.48; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 250 | $5.51 | $3.28 | $-71.50 | $7,627.81 | ▼ -71.50 after sell → book $10,859.20; vs 09:30 mark -3.28 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 58 | $21.64 | $2.18 | $+112.23 | $8,880.74 | ▲ +112.23 after sell → book $10,857.01; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 29 | $32.90 | $2.10 | $+90.66 | $9,832.75 | ▲ +90.66 after sell → book $10,854.92; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 330 | $2.00 | $4.32 | $+73.92 | $10,488.43 | ▲ +73.92 after sell → book $10,850.60; vs 09:30 mark -4.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 2 | $160.93 | $2.02 | $+28.77 | $10,808.27 | ▲ +28.77 after sell → book $10,848.58; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 58 | $40.72 | $2.16 | — | $8,444.35 | — | rank-weighted leftover; list flatten; ret5=+1.8; leftover $2401.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 149 | $14.09 | $2.44 | — | $6,342.50 | — | rank-weighted leftover; list flatten; ret5=+1.1; leftover $2101.61 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 72 | $24.84 | $2.21 | — | $4,551.81 | — | rank-weighted leftover; list flatten; ret5=+13.0; leftover $1801.38 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 579 | $2.59 | $7.47 | — | $3,044.73 | — | rank-weighted leftover; list flatten; ret5=+4.2; leftover $1501.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 14 | $80.97 | $2.03 | — | $1,909.12 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=-1.3; leftover $1200.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 203 | $4.42 | $2.62 | — | $1,009.24 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=-8.6; leftover $900.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 7 | $75.12 | $2.01 | — | $481.39 | — | rank-weighted leftover; list mover_buy; 🔵; ret5=-2.2; leftover $600.46 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $481.39 | ▲ 09:30 equity $10,899.55 vs yday $10,877.13 (+22.42) | 09:30 open · cash $481.39 (unchanged overnight, no fees) · equity $10,899.55 vs prior close $10,877.13 (+22.42) because holdings re-marked: AUTL×2 yday $2.33 → 09:30 $2.32 -0.02; CRDL×2 yday $2.14 → 09:30 $2.09 -0.10; CYPH×1 yday $1.63 → 09:30 $1.75 +0.12; OCUL×1 yday $10.77 → 09:30 $10.63 -0.14; CRMD×1 yday $8.39 → 09:30 $8.49 +0.10; RZLT×1 yday $5.04 → 09:30 $5.07 +0.03; BMEA×2 yday $1.71 → 09:30 $1.74 +0.06; NPWR×1 yday $1.81 → 09:30 $1.83 +0.02; RRC×58 yday $41.55 → 09:30 $41.44 -6.38; CRK×149 yday $14.50 → 09:30 $14.42 -11.92; MOS×72 yday $24.16 → 09:30 $24.00 -11.52; SLI×579 yday $2.61 → 09:30 $2.60 -5.79; ACMR×14 yday $79.11 → 09:30 $81.65 +35.56; GGB×203 yday $4.46 → 09:30 $4.57 +22.33; MT×7 yday $74.53 → 09:30 $74.54 +0.07 | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 2 | $2.32 | $0.07 | $-0.43 | $485.96 | ▼ -0.43 after sell → book $10,899.48; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 2 | $2.09 | $0.07 | $+0.21 | $490.07 | ▲ +0.21 after sell → book $10,899.41; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 1 | $1.75 | $0.04 | $+0.37 | $491.78 | ▲ +0.37 after sell → book $10,899.37; vs 09:30 mark -0.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 1 | $144.70 | $1.45 | — | $345.63 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $196.71 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 8 | $16.95 | $1.38 | — | $208.65 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $147.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 5 | $18.50 | $0.94 | — | $115.21 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $98.36 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 5 | $9.19 | $0.47 | — | $68.79 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $49.18 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.79 | ▼ 09:30 equity $10,723.80 vs yday $10,949.31 (-225.51) | 09:30 open · cash $68.79 (unchanged overnight, no fees) · equity $10,723.80 vs prior close $10,949.31 (-225.51) because holdings re-marked: OCUL×1 yday $10.82 → 09:30 $10.36 -0.46; CRMD×1 yday $8.31 → 09:30 $8.29 -0.02; RZLT×1 yday $4.98 → 09:30 $4.62 -0.36; BMEA×2 yday $1.68 → 09:30 $1.71 +0.06; NPWR×1 yday $1.89 → 09:30 $1.83 -0.06; RRC×58 yday $41.64 → 09:30 $41.11 -30.74; CRK×149 yday $14.62 → 09:30 $14.56 -8.94; MOS×72 yday $23.76 → 09:30 $23.75 -0.72; SLI×579 yday $2.64 → 09:30 $2.51 -75.27; ACMR×14 yday $80.49 → 09:30 $75.10 -75.46; GGB×203 yday $4.70 → 09:30 $4.55 -30.45; MT×7 yday $74.63 → 09:30 $75.07 +3.08; ANF×1 yday $145.75 → 09:30 $148.67 +2.92; BHVN×8 yday $16.12 → 09:30 $15.44 -5.44; BZ×5 yday $18.00 → 09:30 $17.89 -0.55; CAPR×5 yday $10.06 → 09:30 $9.44 -3.10 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $68.79 | ▲ 09:30 equity $10,775.79 vs yday $10,756.12 (+19.67) | 09:30 open · cash $68.79 (unchanged overnight, no fees) · equity $10,775.79 vs prior close $10,756.12 (+19.67) because holdings re-marked: OCUL×1 yday $10.36 → 09:30 $10.49 +0.13; CRMD×1 yday $8.30 → 09:30 $8.26 -0.04; RZLT×1 yday $4.62 → 09:30 $4.69 +0.07; BMEA×2 yday $1.71 → 09:30 $1.65 -0.12; NPWR×1 yday $1.82 → 09:30 $1.78 -0.04; RRC×58 yday $41.78 → 09:30 $41.32 -26.68; CRK×149 yday $14.51 → 09:30 $14.31 -29.80; MOS×72 yday $23.78 → 09:30 $24.00 +15.84; SLI×579 yday $2.51 → 09:30 $2.70 +110.01; ACMR×14 yday $75.02 → 09:30 $71.24 -52.92; GGB×203 yday $4.55 → 09:30 $4.61 +12.18; MT×7 yday $75.06 → 09:30 $74.31 -5.25; ANF×1 yday $149.28 → 09:30 $142.47 -6.81; BHVN×8 yday $15.40 → 09:30 $15.45 +0.40; BZ×5 yday $17.90 → 09:30 $17.37 -2.65; CAPR×5 yday $9.36 → 09:30 $10.43 +5.35 | — |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $79.15 | ▼ -0.67 after sell → book $10,775.66; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.26 | $0.11 | $-0.21 | $87.30 | ▼ -0.21 after sell → book $10,775.55; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 1 | $4.69 | $0.07 | $-0.67 | $91.92 | ▼ -0.67 after sell → book $10,775.48; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 2 | $1.65 | $0.06 | $-0.04 | $95.16 | ▼ -0.04 after sell → book $10,775.42; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 1 | $1.78 | $0.04 | $-0.28 | $96.90 | ▼ -0.28 after sell → book $10,775.38; vs 09:30 mark -0.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.90 | ▲ 09:30 equity $11,002.04 vs yday $10,881.16 (+120.88) | 09:30 open · cash $96.90 (unchanged overnight, no fees) · equity $11,002.04 vs prior close $10,881.16 (+120.88) because holdings re-marked: RRC×58 yday $41.32 → 09:30 $41.94 +35.96; CRK×149 yday $14.90 → 09:30 $15.82 +137.08; MOS×72 yday $24.25 → 09:30 $23.94 -22.32; SLI×579 yday $2.70 → 09:30 $2.67 -17.37; ACMR×14 yday $71.88 → 09:30 $71.44 -6.16; GGB×203 yday $4.61 → 09:30 $4.57 -8.12; MT×7 yday $73.25 → 09:30 $73.22 -0.21; ANF×1 yday $143.00 → 09:30 $142.00 -1.00; BHVN×8 yday $15.45 → 09:30 $15.39 -0.48; BZ×5 yday $17.17 → 09:30 $17.29 +0.60; CAPR×5 yday $10.19 → 09:30 $10.77 +2.90 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.90 | ▼ 09:30 equity $10,984.85 vs yday $11,019.15 (-34.30) | 09:30 open · cash $96.90 (unchanged overnight, no fees) · equity $10,984.85 vs prior close $11,019.15 (-34.30) because holdings re-marked: RRC×58 yday $42.40 → 09:30 $42.10 -17.40; CRK×149 yday $16.02 → 09:30 $15.70 -47.68; MOS×72 yday $24.78 → 09:30 $24.70 -5.76; SLI×579 yday $2.49 → 09:30 $2.49 +0.00; ACMR×14 yday $70.04 → 09:30 $70.52 +6.72; GGB×203 yday $4.69 → 09:30 $4.81 +24.36; MT×7 yday $73.31 → 09:30 $73.86 +3.85; ANF×1 yday $140.68 → 09:30 $139.65 -1.03; BHVN×8 yday $15.74 → 09:30 $15.97 +1.84; BZ×5 yday $17.55 → 09:30 $17.65 +0.50; CAPR×5 yday $10.01 → 09:30 $10.07 +0.30 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 58 | $42.10 | $2.19 | $+75.68 | $2,536.51 | ▲ +75.68 after sell → book $10,982.66; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 72 | $24.70 | $2.23 | $-14.52 | $4,312.68 | ▼ -14.52 after sell → book $10,980.43; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 579 | $2.49 | $7.58 | $-72.95 | $5,746.81 | ▼ -72.95 after sell → book $10,972.85; vs 09:30 mark -7.58 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ACMR` | 14 | $70.52 | $2.05 | $-150.38 | $6,732.04 | ▼ -150.38 after sell → book $10,970.80; vs 09:30 mark -2.05 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GGB` | 203 | $4.81 | $2.66 | $+73.89 | $7,705.81 | ▲ +73.89 after sell → book $10,968.14; vs 09:30 mark -2.66 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MT` | 7 | $73.86 | $2.03 | $-12.86 | $8,220.80 | ▼ -12.86 after sell → book $10,966.11; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 41 | $49.76 | $2.11 | — | $6,178.52 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2055.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 42 | $41.31 | $2.12 | — | $4,441.39 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1761.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 448 | $3.27 | $5.78 | — | $2,970.65 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1468.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 152 | $7.70 | $2.45 | — | $1,797.80 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1174.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $125.94 | $2.01 | — | $1,040.15 | — | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $880.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 481 | $1.22 | $6.20 | — | $447.13 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $587.20 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 15 | $18.40 | $2.04 | — | $169.09 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $293.60 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $169.09 | ▲ 09:30 equity $11,614.53 vs yday $11,525.56 (+88.97) | 09:30 open · cash $169.09 (unchanged overnight, no fees) · equity $11,614.53 vs prior close $11,525.56 (+88.97) because holdings re-marked: CRK×149 yday $15.54 → 09:30 $15.45 -13.41; ANF×1 yday $136.60 → 09:30 $137.70 +1.10; BHVN×8 yday $15.69 → 09:30 $15.89 +1.60; BZ×5 yday $17.30 → 09:30 $17.31 +0.05; CAPR×5 yday $9.89 → 09:30 $9.83 -0.30; ATRC×41 yday $52.59 → 09:30 $52.88 +11.89; HRMY×42 yday $42.86 → 09:30 $42.93 +2.94; CABA×448 yday $3.57 → 09:30 $3.63 +26.88; VSTM×152 yday $8.02 → 09:30 $8.03 +1.52; RVTY×6 yday $130.94 → 09:30 $132.45 +9.06; GPRO×481 yday $1.69 → 09:30 $1.78 +43.29; FRVO×15 yday $17.98 → 09:30 $18.27 +4.35 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 149 | $15.45 | $2.48 | $+197.72 | $2,468.66 | ▲ +197.72 after sell → book $11,612.05; vs 09:30 mark -2.48 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 1 | $137.70 | $1.40 | $-9.85 | $2,604.96 | ▼ -9.85 after sell → book $11,610.65; vs 09:30 mark -1.40 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 8 | $15.89 | $1.32 | $-11.18 | $2,730.77 | ▼ -11.18 after sell → book $11,609.34; vs 09:30 mark -1.31 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 5 | $17.31 | $0.90 | $-7.79 | $2,816.42 | ▼ -7.79 after sell → book $11,608.44; vs 09:30 mark -0.90 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 5 | $9.83 | $0.53 | $+2.20 | $2,865.04 | ▲ +2.20 after sell → book $11,607.91; vs 09:30 mark -0.53 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 3 | $266.94 | $2.00 | — | $2,062.22 | — | rank-weighted leftover; list flatten; ret5=+1.9; leftover $955.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 24 | $30.65 | $2.06 | — | $1,324.56 | — | rank-weighted leftover; list flatten; 🔵; ret5=-2.2; leftover $764.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 55 | $10.41 | $2.15 | — | $749.85 | — | rank-weighted leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $573.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 26 | $14.50 | $2.07 | — | $370.79 | — | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $382.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 97 | $1.95 | $2.18 | — | $179.45 | — | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $191.00 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 28.46 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 24.90 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 21.34 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 17.78 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 14.23 < 1 share @ 57.61 |
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
| 2026-08-17 | `DVN` | cash | leftover split 24.28 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 21.25 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 18.21 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.11 < 1 share @ 90.54 |
| 2026-08-17 | `HNST` | cash | leftover split 3.04 < 1 share @ 4.81 |
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
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 14.68 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 12.84 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 11.01 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.17 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 3.67 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 18.21 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 13.66 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 6.83 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 300.23 < 1 share @ 925.74 |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `ATRC` | 41 | 2026-09-03 @ $49.76 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2055.20 |
| `HRMY` | 42 | 2026-09-03 @ $41.31 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1761.60 |
| `CABA` | 448 | 2026-09-03 @ $3.27 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1468.00 |
| `VSTM` | 152 | 2026-09-03 @ $7.70 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1174.40 |
| `RVTY` | 6 | 2026-09-03 @ $125.94 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $880.80 |
| `GPRO` | 481 | 2026-09-03 @ $1.22 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $587.20 |
| `FRVO` | 15 | 2026-09-03 @ $18.40 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $293.60 |
| `ASND` | 3 | 2026-09-04 @ $266.94 | rank-weighted leftover; list flatten; ret5=+1.9; leftover $955.01 |
| `OSCR` | 24 | 2026-09-04 @ $30.65 | rank-weighted leftover; list flatten; 🔵; ret5=-2.2; leftover $764.01 |
| `NVAX` | 55 | 2026-09-04 @ $10.41 | rank-weighted leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $573.01 |
| `BVS` | 26 | 2026-09-04 @ $14.50 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $382.01 |
| `BAK` | 97 | 2026-09-04 @ $1.95 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $191.00 |
