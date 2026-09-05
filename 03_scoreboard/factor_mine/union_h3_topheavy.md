# Factor mine action — `union_h3_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+10.51%** ($11,051) · signal-only (no cash/fees) was +34.19%. Starts YES **15/17**. Fills 96 · skips 155 · realized $+688.25.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $68.26.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $160.57 | $10,123.05 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $160.57 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | $10,109.78 | -13.27 | MARA, LDI, BTBT | — | $124.52 | $10,395.68 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9 | 09:30 open · cash $160.57 (unchanged overnight, no fees) · equity $10,109.78 vs prior close $10,123.05 (-13.27) because holdings re-marked: BTSG×66 yday $60.23 → 09:30 $59.65 -38.28; IREN×18 yday $44.76 → 09:30 $44.09 -12.06; TPG×16 yday $54.62 → 09:30 $55.29 +10.72; TGTX×17 yday $47.94 → 09:30 $47.27 -11.39; SLS×73 yday $12.36 → 09:30 $12.40 +2.92; HIMS×28 yday $28.77 → 09:30 $29.15 +10.64; INO×1058 yday $0.90 → 09:30 $0.93 +31.74; TNDM×36 yday $23.13 → 09:30 $22.92 -7.56 |
| 2026-08-17 | +2.25 | $124.52 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9 | $10,380.01 | -15.67 | DVN, TMC, TGB, DNN, HNST | — | $41.59 | $10,388.13 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $124.52 (unchanged overnight, no fees) · equity $10,380.01 vs prior close $10,395.68 (-15.67) because holdings re-marked: BTSG×66 yday $61.71 → 09:30 $61.69 -1.32; IREN×18 yday $44.06 → 09:30 $45.23 +21.06; TPG×16 yday $53.03 → 09:30 $52.67 -5.76; TGTX×17 yday $48.74 → 09:30 $48.74 +0.00; SLS×73 yday $12.78 → 09:30 $12.78 +0.00; HIMS×28 yday $28.15 → 09:30 $28.14 -0.28; INO×1058 yday $1.09 → 09:30 $1.07 -21.16; TNDM×36 yday $22.72 → 09:30 $22.50 -7.92; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×14 yday $0.90 → 09:30 $0.91 +0.14; BTBT×9 yday $1.57 → 09:30 $1.52 -0.45 |
| 2026-08-18 | -6.20 | $41.59 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | $10,277.67 | -110.46 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,130.92 | $10,247.92 | MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $41.59 (unchanged overnight, no fees) · equity $10,277.67 vs prior close $10,388.13 (-110.46) because holdings re-marked: BTSG×66 yday $60.38 → 09:30 $60.00 -25.08; IREN×18 yday $44.90 → 09:30 $43.56 -24.12; TPG×16 yday $51.77 → 09:30 $51.77 +0.00; TGTX×17 yday $49.28 → 09:30 $49.28 +0.00; SLS×73 yday $13.00 → 09:30 $12.66 -24.82; HIMS×28 yday $28.61 → 09:30 $27.85 -21.28; INO×1058 yday $1.15 → 09:30 $1.14 -10.58; TNDM×36 yday $22.25 → 09:30 $22.16 -3.42; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×14 yday $0.88 → 09:30 $0.87 -0.07; BTBT×9 yday $1.60 → 09:30 $1.54 -0.54; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×2 yday $3.77 → 09:30 $3.72 -0.10; TGB×1 yday $8.77 → 09:30 $8.55 -0.22; DNN×3 yday $3.19 → 09:30 $3.11 -0.24; HNST×2 yday $4.70 → 09:30 $4.67 -0.06 |
| 2026-08-19 | -7.20 | $10,130.92 | MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | $10,248.88 | +0.96 | — | MARA, LDI, BTBT | $10,164.46 | $10,248.76 | DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | 09:30 open · cash $10,130.92 (unchanged overnight, no fees) · equity $10,248.88 vs prior close $10,247.92 (+0.96) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×14 yday $0.86 → 09:30 $0.88 +0.31; BTBT×9 yday $1.45 → 09:30 $1.42 -0.27; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 |
| 2026-08-20 | +1.12 | $10,164.46 | DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | $10,249.23 | +0.47 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, TMC, TGB, DNN, HNST | $106.56 | $10,491.02 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6 | 09:30 open · cash $10,164.46 (unchanged overnight, no fees) · equity $10,249.23 vs prior close $10,248.76 (+0.47) because holdings re-marked: DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 |
| 2026-08-21 | +3.25 | $106.56 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6 | $10,790.88 | +299.86 | AUTL, CRDL, CYPH | — | $83.24 | $10,661.83 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6, AUTL×3, CRDL×4, CYPH×6 | 09:30 open · cash $106.56 (unchanged overnight, no fees) · equity $10,790.88 vs prior close $10,491.02 (+299.86) because holdings re-marked: AG×199 yday $21.19 → 09:30 $21.90 +141.29; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×42 yday $21.11 → 09:30 $21.75 +26.88; HDSN×152 yday $5.57 → 09:30 $5.67 +15.20; IAG×44 yday $20.50 → 09:30 $21.17 +29.48; KGC×29 yday $31.43 → 09:30 $32.17 +21.46; NFGC×501 yday $1.75 → 09:30 $1.79 +20.04; WPM×6 yday $150.25 → 09:30 $154.70 +26.70 |
| 2026-08-24 | -5.17 | $83.24 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6, AUTL×3, CRDL×4, CYPH×6 | $10,807.24 | +145.41 | — | — | $83.24 | $10,559.38 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6, AUTL×3, CRDL×4, CYPH×6 | 09:30 open · cash $83.24 (unchanged overnight, no fees) · equity $10,807.24 vs prior close $10,661.83 (+145.41) because holdings re-marked: AG×199 yday $21.09 → 09:30 $21.47 +75.62; BHP×9 yday $97.03 → 09:30 $97.34 +2.79; CDE×42 yday $20.97 → 09:30 $21.26 +12.18; HDSN×152 yday $5.63 → 09:30 $5.69 +9.12; IAG×44 yday $21.14 → 09:30 $21.44 +13.20; KGC×29 yday $32.76 → 09:30 $33.21 +13.05; NFGC×501 yday $1.84 → 09:30 $1.86 +10.02; WPM×6 yday $157.78 → 09:30 $158.96 +7.08; AUTL×3 yday $2.41 → 09:30 $2.36 -0.15; CRDL×4 yday $1.86 → 09:30 $1.87 +0.04; CYPH×6 yday $1.42 → 09:30 $1.83 +2.46 |
| 2026-08-25 | +1.80 | $83.24 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6, AUTL×3, CRDL×4, CYPH×6 | $10,631.75 | +72.37 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $88.25 | $10,551.00 | AUTL×3, CRDL×4, CYPH×6, MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453 | 09:30 open · cash $83.24 (unchanged overnight, no fees) · equity $10,631.75 vs prior close $10,559.38 (+72.37) because holdings re-marked: AG×199 yday $20.57 → 09:30 $20.73 +31.84; BHP×9 yday $96.66 → 09:30 $95.95 -6.39; CDE×42 yday $20.49 → 09:30 $20.85 +15.12; HDSN×152 yday $5.57 → 09:30 $5.53 -6.08; IAG×44 yday $21.36 → 09:30 $21.63 +11.88; KGC×29 yday $32.47 → 09:30 $32.76 +8.41; NFGC×501 yday $1.90 → 09:30 $1.91 +5.01; WPM×6 yday $158.00 → 09:30 $160.00 +12.00; AUTL×3 yday $2.38 → 09:30 $2.32 -0.18; CRDL×4 yday $1.80 → 09:30 $1.90 +0.40; CYPH×6 yday $1.64 → 09:30 $1.70 +0.36 |
| 2026-08-26 | +2.02 | $88.25 | AUTL×3, CRDL×4, CYPH×6, MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453 | $10,551.00 | -0.00 | — | — | $88.25 | $10,582.94 | AUTL×3, CRDL×4, CYPH×6, MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453 | 09:30 open · cash $88.25 (unchanged overnight, no fees) · equity $10,551.00 vs prior close $10,551.00 (-0.00) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.34 +0.00; CRDL×4 yday $1.90 → 09:30 $1.90 +0.00; CYPH×6 yday $1.64 → 09:30 $1.64 +0.00; MOS×176 yday $23.75 → 09:30 $23.75 +0.00; OCUL×83 yday $10.92 → 09:30 $10.92 +0.00; INSP×14 yday $61.47 → 09:30 $61.47 +0.00; CRMD×109 yday $8.28 → 09:30 $8.28 +0.00; RZLT×173 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×560 yday $1.61 → 09:30 $1.61 +0.00; NPWR×453 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $88.25 | AUTL×3, CRDL×4, CYPH×6, MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453 | $10,735.01 | +152.07 | RRC, SLI, GGB | AUTL, CRDL, CYPH | $52.31 | $10,542.77 | MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453, RRC×1, SLI×4, GGB×2 | 09:30 open · cash $88.25 (unchanged overnight, no fees) · equity $10,735.01 vs prior close $10,582.94 (+152.07) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.41 +0.21; CRDL×4 yday $1.90 → 09:30 $2.03 +0.52; CYPH×6 yday $1.64 → 09:30 $1.60 -0.24; MOS×176 yday $23.75 → 09:30 $24.84 +191.84; OCUL×83 yday $10.92 → 09:30 $10.79 -10.79; INSP×14 yday $61.47 → 09:30 $60.07 -19.60; CRMD×109 yday $8.28 → 09:30 $8.60 +34.88; RZLT×173 yday $5.29 → 09:30 $5.01 -48.44; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×560 yday $1.61 → 09:30 $1.75 +78.40; NPWR×453 yday $2.02 → 09:30 $1.93 -40.77 |
| 2026-08-28 | +0.75 | $52.31 | MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453, RRC×1, SLI×4, GGB×2 | $10,544.11 | +1.34 | CRK, ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $89.67 | $10,524.83 | MOS×176, RRC×1, SLI×4, GGB×2, CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | 09:30 open · cash $52.31 (unchanged overnight, no fees) · equity $10,544.11 vs prior close $10,542.77 (+1.34) because holdings re-marked: MOS×176 yday $24.16 → 09:30 $24.00 -28.16; OCUL×83 yday $10.77 → 09:30 $10.63 -11.62; INSP×14 yday $61.80 → 09:30 $62.10 +4.20; CRMD×109 yday $8.39 → 09:30 $8.49 +10.90; RZLT×173 yday $5.04 → 09:30 $5.07 +5.19; HCA×2 yday $427.16 → 09:30 $424.61 -5.10; BMEA×560 yday $1.71 → 09:30 $1.74 +16.80; NPWR×453 yday $1.81 → 09:30 $1.83 +9.06; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; SLI×4 yday $2.61 → 09:30 $2.60 -0.04; GGB×2 yday $4.46 → 09:30 $4.57 +0.22 |
| 2026-08-31 | -5.85 | $89.67 | MOS×176, RRC×1, SLI×4, GGB×2, CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | $10,423.40 | -101.43 | — | MOS | $4,267.09 | $10,406.77 | RRC×1, SLI×4, GGB×2, CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | 09:30 open · cash $89.67 (unchanged overnight, no fees) · equity $10,423.40 vs prior close $10,524.83 (-101.43) because holdings re-marked: MOS×176 yday $23.76 → 09:30 $23.75 -1.76; RRC×1 yday $41.64 → 09:30 $41.11 -0.53; SLI×4 yday $2.64 → 09:30 $2.51 -0.52; GGB×2 yday $4.70 → 09:30 $4.55 -0.30; CRK×172 yday $14.62 → 09:30 $14.56 -10.32; ANF×6 yday $145.75 → 09:30 $148.67 +17.52; BHVN×55 yday $16.12 → 09:30 $15.44 -37.40; BZ×50 yday $18.00 → 09:30 $17.89 -5.50; CAPR×101 yday $10.06 → 09:30 $9.44 -62.62 |
| 2026-09-01 | -6.30 | $4,267.09 | RRC×1, SLI×4, GGB×2, CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | $10,416.25 | +9.48 | — | RRC, SLI, GGB | $4,327.73 | $10,485.97 | CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | 09:30 open · cash $4,267.09 (unchanged overnight, no fees) · equity $10,416.25 vs prior close $10,406.77 (+9.48) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; SLI×4 yday $2.51 → 09:30 $2.70 +0.76; GGB×2 yday $4.55 → 09:30 $4.61 +0.12; CRK×172 yday $14.51 → 09:30 $14.31 -34.40; ANF×6 yday $149.28 → 09:30 $142.47 -40.86; BHVN×55 yday $15.40 → 09:30 $15.45 +2.75; BZ×50 yday $17.90 → 09:30 $17.37 -26.50; CAPR×101 yday $9.36 → 09:30 $10.43 +108.07 |
| 2026-09-02 | -3.83 | $4,327.73 | CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | $10,699.49 | +213.52 | — | CRK, ANF, BHVN, BZ, CAPR | $10,688.25 | $10,688.25 | — | 09:30 open · cash $4,327.73 (unchanged overnight, no fees) · equity $10,699.49 vs prior close $10,485.97 (+213.52) because holdings re-marked: CRK×172 yday $14.90 → 09:30 $15.82 +158.24; ANF×6 yday $143.00 → 09:30 $142.00 -6.00; BHVN×55 yday $15.45 → 09:30 $15.39 -3.30; BZ×50 yday $17.17 → 09:30 $17.29 +6.00; CAPR×101 yday $10.19 → 09:30 $10.77 +58.58 |
| 2026-09-03 | -0.90 | $10,688.25 | — | $10,688.25 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $90.61 | $11,416.06 | ATRC×85, HRMY×22, CABA×280, VSTM×118, RVTY×7, GPRO×750, FRVO×49, CRK×58 | 09:30 open · cash $10,688.25 · no holdings · equity $10,688.25 vs prior close $10,688.25 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $90.61 | ATRC×85, HRMY×22, CABA×280, VSTM×118, RVTY×7, GPRO×750, FRVO×49, CRK×58 | $11,547.29 | +131.23 | NVAX, BAK | — | $68.26 | $11,051.15 | ATRC×85, HRMY×22, CABA×280, VSTM×118, RVTY×7, GPRO×750, FRVO×49, CRK×58, NVAX×1, BAK×6 | 09:30 open · cash $90.61 (unchanged overnight, no fees) · equity $11,547.29 vs prior close $11,416.06 (+131.23) because holdings re-marked: ATRC×85 yday $52.59 → 09:30 $52.88 +24.65; HRMY×22 yday $42.86 → 09:30 $42.93 +1.54; CABA×280 yday $3.57 → 09:30 $3.63 +16.80; VSTM×118 yday $8.02 → 09:30 $8.03 +1.18; RVTY×7 yday $130.94 → 09:30 $132.45 +10.57; GPRO×750 yday $1.69 → 09:30 $1.78 +67.50; FRVO×49 yday $17.98 → 09:30 $18.27 +14.21; CRK×58 yday $15.54 → 09:30 $15.45 -5.22 |

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
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 66 | $60.00 | $2.23 | $+8.78 | $3,999.36 | ▲ +8.78 after sell → book $10,275.44; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 18 | $43.56 | $2.06 | $-47.67 | $4,781.37 | ▼ -47.67 after sell → book $10,273.37; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 16 | $51.77 | $2.06 | $+14.25 | $5,607.64 | ▲ +14.25 after sell → book $10,271.32; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 17 | $49.28 | $2.06 | $-11.24 | $6,443.34 | ▼ -11.24 after sell → book $10,269.26; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 73 | $12.66 | $2.23 | $+65.64 | $7,365.28 | ▲ +65.64 after sell → book $10,267.02; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 28 | $27.85 | $2.09 | $-57.09 | $8,142.99 | ▼ -57.09 after sell → book $10,264.93; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1058 | $1.14 | $13.83 | $+323.56 | $9,335.28 | ▲ +323.56 after sell → book $10,251.10; vs 09:30 mark -13.83 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 36 | $22.16 | $2.12 | $-46.34 | $10,130.92 | ▼ -46.34 after sell → book $10,248.98; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,130.92 | ▲ 09:30 equity $10,248.88 vs yday $10,247.92 (+0.96) | 09:30 open · cash $10,130.92 (unchanged overnight, no fees) · equity $10,248.88 vs prior close $10,247.92 (+0.96) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×14 yday $0.86 → 09:30 $0.88 +0.31; BTBT×9 yday $1.45 → 09:30 $1.42 -0.27; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×2 yday $3.92 → 09:30 $3.93 +0.02; TGB×1 yday $8.36 → 09:30 $8.70 +0.34; DNN×3 yday $3.15 → 09:30 $3.19 +0.12; HNST×2 yday $4.75 → 09:30 $4.80 +0.10 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,139.72 | ▼ -0.31 after sell → book $10,248.77; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 14 | $0.88 | $0.19 | $-1.16 | $10,151.85 | ▼ -1.16 after sell → book $10,248.58; vs 09:30 mark -0.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 9 | $1.42 | $0.17 | $-1.06 | $10,164.46 | ▼ -1.06 after sell → book $10,248.41; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,164.46 | ▲ 09:30 equity $10,249.23 vs yday $10,248.76 (+0.47) | 09:30 open · cash $10,164.46 (unchanged overnight, no fees) · equity $10,249.23 vs prior close $10,248.76 (+0.47) because holdings re-marked: DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×2 yday $3.97 → 09:30 $3.92 -0.10; TGB×1 yday $8.47 → 09:30 $8.35 -0.12; DNN×3 yday $3.22 → 09:30 $3.20 -0.06; HNST×2 yday $5.02 → 09:30 $4.98 -0.08 | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $10,212.96 | ▲ +1.86 after sell → book $10,248.71; vs 09:30 mark -0.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $10,220.70 | ▼ -0.45 after sell → book $10,248.61; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,228.94 | ▼ -0.30 after sell → book $10,248.50; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 3 | $3.20 | $0.12 | $-0.35 | $10,238.42 | ▼ -0.35 after sell → book $10,248.38; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 2 | $4.98 | $0.13 | $+0.11 | $10,248.25 | ▲ +0.11 after sell → book $10,248.25; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 199 | $20.55 | $2.59 | — | $6,156.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $4099.30 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,335.11 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 42 | $20.65 | $2.12 | — | $4,465.69 | — | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 152 | $5.77 | $2.45 | — | $3,586.21 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 44 | $19.63 | $2.12 | — | $2,720.36 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $1,859.02 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 501 | $1.75 | $6.46 | — | $975.80 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $106.56 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $106.56 | ▲ 09:30 equity $10,790.88 vs yday $10,491.02 (+299.86) | 09:30 open · cash $106.56 (unchanged overnight, no fees) · equity $10,790.88 vs prior close $10,491.02 (+299.86) because holdings re-marked: AG×199 yday $21.19 → 09:30 $21.90 +141.29; BHP×9 yday $93.63 → 09:30 $95.72 +18.81; CDE×42 yday $21.11 → 09:30 $21.75 +26.88; HDSN×152 yday $5.57 → 09:30 $5.67 +15.20; IAG×44 yday $20.50 → 09:30 $21.17 +29.48; KGC×29 yday $31.43 → 09:30 $32.17 +21.46; NFGC×501 yday $1.75 → 09:30 $1.79 +20.04; WPM×6 yday $150.25 → 09:30 $154.70 +26.70 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 3 | $2.47 | $0.08 | — | $99.06 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $9.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 4 | $1.93 | $0.09 | — | $91.25 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $9.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 6 | $1.32 | $0.10 | — | $83.24 | — | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $9.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.24 | ▲ 09:30 equity $10,807.24 vs yday $10,661.83 (+145.41) | 09:30 open · cash $83.24 (unchanged overnight, no fees) · equity $10,807.24 vs prior close $10,661.83 (+145.41) because holdings re-marked: AG×199 yday $21.09 → 09:30 $21.47 +75.62; BHP×9 yday $97.03 → 09:30 $97.34 +2.79; CDE×42 yday $20.97 → 09:30 $21.26 +12.18; HDSN×152 yday $5.63 → 09:30 $5.69 +9.12; IAG×44 yday $21.14 → 09:30 $21.44 +13.20; KGC×29 yday $32.76 → 09:30 $33.21 +13.05; NFGC×501 yday $1.84 → 09:30 $1.86 +10.02; WPM×6 yday $157.78 → 09:30 $158.96 +7.08; AUTL×3 yday $2.41 → 09:30 $2.36 -0.15; CRDL×4 yday $1.86 → 09:30 $1.87 +0.04; CYPH×6 yday $1.42 → 09:30 $1.83 +2.46 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $83.24 | ▲ 09:30 equity $10,631.75 vs yday $10,559.38 (+72.37) | 09:30 open · cash $83.24 (unchanged overnight, no fees) · equity $10,631.75 vs prior close $10,559.38 (+72.37) because holdings re-marked: AG×199 yday $20.57 → 09:30 $20.73 +31.84; BHP×9 yday $96.66 → 09:30 $95.95 -6.39; CDE×42 yday $20.49 → 09:30 $20.85 +15.12; HDSN×152 yday $5.57 → 09:30 $5.53 -6.08; IAG×44 yday $21.36 → 09:30 $21.63 +11.88; KGC×29 yday $32.47 → 09:30 $32.76 +8.41; NFGC×501 yday $1.90 → 09:30 $1.91 +5.01; WPM×6 yday $158.00 → 09:30 $160.00 +12.00; AUTL×3 yday $2.38 → 09:30 $2.32 -0.18; CRDL×4 yday $1.80 → 09:30 $1.90 +0.40; CYPH×6 yday $1.64 → 09:30 $1.70 +0.36 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 199 | $20.73 | $2.65 | $+30.58 | $4,205.85 | ▲ +30.58 after sell → book $10,629.09; vs 09:30 mark -2.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 9 | $95.95 | $2.04 | $+40.41 | $5,067.37 | ▲ +40.41 after sell → book $10,627.06; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 42 | $20.85 | $2.14 | $+4.15 | $5,940.93 | ▲ +4.15 after sell → book $10,624.92; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 152 | $5.53 | $2.48 | $-41.41 | $6,779.01 | ▼ -41.41 after sell → book $10,622.44; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 44 | $21.63 | $2.14 | $+83.74 | $7,728.59 | ▲ +83.74 after sell → book $10,620.30; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 29 | $32.76 | $2.10 | $+86.60 | $8,676.53 | ▲ +86.60 after sell → book $10,618.20; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 501 | $1.91 | $6.56 | $+67.14 | $9,626.88 | ▲ +67.14 after sell → book $10,611.64; vs 09:30 mark -6.56 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 6 | $160.00 | $2.03 | $+88.72 | $10,584.86 | ▲ +88.72 after sell → book $10,609.62; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 176 | $24.00 | $2.52 | — | $6,358.34 | — | 40% to #1, rest split; list flatten; ⚪; ret5=+13.0; leftover $4233.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 83 | $10.92 | $2.24 | — | $5,449.74 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+10.4; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 14 | $61.47 | $2.03 | — | $4,587.13 | — | 40% to #1, rest split; list flatten; 🔵; ret5=+9.2; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 109 | $8.28 | $2.32 | — | $3,682.29 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.8; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 173 | $5.23 | $2.51 | — | $2,774.99 | — | 40% to #1, rest split; list flatten; ret5=+10.7; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $1,914.51 | — | 40% to #1, rest split; list flatten; ret5=+6.1; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 560 | $1.62 | $7.22 | — | $1,000.09 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 453 | $2.00 | $5.84 | — | $88.25 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $907.27 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.25 | ▲ 09:30 equity $10,551.00 vs yday $10,551.00 (-0.00) | 09:30 open · cash $88.25 (unchanged overnight, no fees) · equity $10,551.00 vs prior close $10,551.00 (-0.00) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.34 +0.00; CRDL×4 yday $1.90 → 09:30 $1.90 +0.00; CYPH×6 yday $1.64 → 09:30 $1.64 +0.00; MOS×176 yday $23.75 → 09:30 $23.75 +0.00; OCUL×83 yday $10.92 → 09:30 $10.92 +0.00; INSP×14 yday $61.47 → 09:30 $61.47 +0.00; CRMD×109 yday $8.28 → 09:30 $8.28 +0.00; RZLT×173 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×560 yday $1.61 → 09:30 $1.61 +0.00; NPWR×453 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $88.25 | ▲ 09:30 equity $10,735.01 vs yday $10,582.94 (+152.07) | 09:30 open · cash $88.25 (unchanged overnight, no fees) · equity $10,735.01 vs prior close $10,582.94 (+152.07) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.41 +0.21; CRDL×4 yday $1.90 → 09:30 $2.03 +0.52; CYPH×6 yday $1.64 → 09:30 $1.60 -0.24; MOS×176 yday $23.75 → 09:30 $24.84 +191.84; OCUL×83 yday $10.92 → 09:30 $10.79 -10.79; INSP×14 yday $61.47 → 09:30 $60.07 -19.60; CRMD×109 yday $8.28 → 09:30 $8.60 +34.88; RZLT×173 yday $5.29 → 09:30 $5.01 -48.44; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×560 yday $1.61 → 09:30 $1.75 +78.40; NPWR×453 yday $2.02 → 09:30 $1.93 -40.77 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 3 | $2.41 | $0.10 | $-0.36 | $95.38 | ▼ -0.36 after sell → book $10,734.91; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 4 | $2.03 | $0.11 | $+0.20 | $103.38 | ▲ +0.20 after sell → book $10,734.79; vs 09:30 mark -0.12 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 6 | $1.60 | $0.13 | $+1.45 | $112.85 | ▲ +1.45 after sell → book $10,734.66; vs 09:30 mark -0.13 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $71.72 | — | 40% to #1, rest split; list flatten; ret5=+1.8; leftover $45.14 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 4 | $2.59 | $0.12 | — | $61.24 | — | 40% to #1, rest split; list flatten; ret5=+4.2; leftover $11.28 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 2 | $4.42 | $0.09 | — | $52.31 | — | 40% to #1, rest split; list mover_buy; 🔵; ret5=-8.6; leftover $11.28 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.31 | ▲ 09:30 equity $10,544.11 vs yday $10,542.77 (+1.34) | 09:30 open · cash $52.31 (unchanged overnight, no fees) · equity $10,544.11 vs prior close $10,542.77 (+1.34) because holdings re-marked: MOS×176 yday $24.16 → 09:30 $24.00 -28.16; OCUL×83 yday $10.77 → 09:30 $10.63 -11.62; INSP×14 yday $61.80 → 09:30 $62.10 +4.20; CRMD×109 yday $8.39 → 09:30 $8.49 +10.90; RZLT×173 yday $5.04 → 09:30 $5.07 +5.19; HCA×2 yday $427.16 → 09:30 $424.61 -5.10; BMEA×560 yday $1.71 → 09:30 $1.74 +16.80; NPWR×453 yday $1.81 → 09:30 $1.83 +9.06; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; SLI×4 yday $2.61 → 09:30 $2.60 -0.04; GGB×2 yday $4.46 → 09:30 $4.57 +0.22 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 83 | $10.63 | $2.26 | $-28.57 | $932.34 | ▼ -28.57 after sell → book $10,541.85; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 14 | $62.10 | $2.05 | $+4.74 | $1,799.68 | ▲ +4.74 after sell → book $10,539.79; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 109 | $8.49 | $2.35 | $+18.23 | $2,722.75 | ▲ +18.23 after sell → book $10,537.45; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 173 | $5.07 | $2.55 | $-32.74 | $3,597.31 | ▼ -32.74 after sell → book $10,534.90; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-13.27 | $4,444.51 | ▼ -13.27 after sell → book $10,532.88; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 560 | $1.74 | $7.33 | $+52.65 | $5,411.59 | ▲ +52.65 after sell → book $10,525.56; vs 09:30 mark -7.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 453 | $1.83 | $5.93 | $-88.78 | $6,234.65 | ▼ -88.78 after sell → book $10,519.63; vs 09:30 mark -5.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 172 | $14.42 | $2.51 | — | $3,751.90 | — | 40% to #1, rest split; list flatten; ret5=+1.1; leftover $2493.86 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $144.70 | $2.01 | — | $2,881.69 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $935.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 55 | $16.95 | $2.15 | — | $1,947.29 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $935.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 50 | $18.50 | $2.14 | — | $1,020.15 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $935.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 101 | $9.19 | $2.29 | — | $89.67 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $935.20 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.67 | ▼ 09:30 equity $10,423.40 vs yday $10,524.83 (-101.43) | 09:30 open · cash $89.67 (unchanged overnight, no fees) · equity $10,423.40 vs prior close $10,524.83 (-101.43) because holdings re-marked: MOS×176 yday $23.76 → 09:30 $23.75 -1.76; RRC×1 yday $41.64 → 09:30 $41.11 -0.53; SLI×4 yday $2.64 → 09:30 $2.51 -0.52; GGB×2 yday $4.70 → 09:30 $4.55 -0.30; CRK×172 yday $14.62 → 09:30 $14.56 -10.32; ANF×6 yday $145.75 → 09:30 $148.67 +17.52; BHVN×55 yday $16.12 → 09:30 $15.44 -37.40; BZ×50 yday $18.00 → 09:30 $17.89 -5.50; CAPR×101 yday $10.06 → 09:30 $9.44 -62.62 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 176 | $23.75 | $2.58 | $-49.10 | $4,267.09 | ▼ -49.10 after sell → book $10,420.82; vs 09:30 mark -2.58 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,267.09 | ▲ 09:30 equity $10,416.25 vs yday $10,406.77 (+9.48) | 09:30 open · cash $4,267.09 (unchanged overnight, no fees) · equity $10,416.25 vs prior close $10,406.77 (+9.48) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; SLI×4 yday $2.51 → 09:30 $2.70 +0.76; GGB×2 yday $4.55 → 09:30 $4.61 +0.12; CRK×172 yday $14.51 → 09:30 $14.31 -34.40; ANF×6 yday $149.28 → 09:30 $142.47 -40.86; BHVN×55 yday $15.40 → 09:30 $15.45 +2.75; BZ×50 yday $17.90 → 09:30 $17.37 -26.50; CAPR×101 yday $9.36 → 09:30 $10.43 +108.07 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $4,307.97 | ▼ -0.25 after sell → book $10,415.81; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 4 | $2.70 | $0.14 | $+0.18 | $4,318.63 | ▲ +0.18 after sell → book $10,415.67; vs 09:30 mark -0.14 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 2 | $4.61 | $0.12 | $+0.17 | $4,327.73 | ▲ +0.17 after sell → book $10,415.55; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4,327.73 | ▲ 09:30 equity $10,699.49 vs yday $10,485.97 (+213.52) | 09:30 open · cash $4,327.73 (unchanged overnight, no fees) · equity $10,699.49 vs prior close $10,485.97 (+213.52) because holdings re-marked: CRK×172 yday $14.90 → 09:30 $15.82 +158.24; ANF×6 yday $143.00 → 09:30 $142.00 -6.00; BHVN×55 yday $15.45 → 09:30 $15.39 -3.30; BZ×50 yday $17.17 → 09:30 $17.29 +6.00; CAPR×101 yday $10.19 → 09:30 $10.77 +58.58 | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 172 | $15.82 | $2.56 | $+235.74 | $7,046.22 | ▲ +235.74 after sell → book $10,696.94; vs 09:30 mark -2.55 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 6 | $142.00 | $2.03 | $-20.24 | $7,896.19 | ▼ -20.24 after sell → book $10,694.91; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 55 | $15.39 | $2.17 | $-90.13 | $8,740.46 | ▼ -90.13 after sell → book $10,692.73; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 50 | $17.29 | $2.16 | $-64.80 | $9,602.80 | ▼ -64.80 after sell → book $10,690.57; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 101 | $10.77 | $2.32 | $+154.97 | $10,688.25 | ▲ +154.97 after sell → book $10,688.25; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,688.25 | ▲ 09:30 equity $10,688.25 vs yday $10,688.25 (+0.00) | 09:30 open · cash $10,688.25 · no holdings · equity $10,688.25 vs prior close $10,688.25 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 85 | $49.76 | $2.25 | — | $6,456.41 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+10.6; leftover $4275.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 22 | $41.31 | $2.06 | — | $5,545.53 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $916.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 280 | $3.27 | $3.61 | — | $4,626.32 | — | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+13.8; leftover $916.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 118 | $7.70 | $2.34 | — | $3,715.38 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $916.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $125.94 | $2.01 | — | $2,831.78 | — | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $916.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 750 | $1.22 | $9.68 | — | $1,907.11 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $916.14 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 49 | $18.40 | $2.14 | — | $1,003.37 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $916.14 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 58 | $15.70 | $2.16 | — | $90.61 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $916.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $90.61 | ▲ 09:30 equity $11,547.29 vs yday $11,416.06 (+131.23) | 09:30 open · cash $90.61 (unchanged overnight, no fees) · equity $11,547.29 vs prior close $11,416.06 (+131.23) because holdings re-marked: ATRC×85 yday $52.59 → 09:30 $52.88 +24.65; HRMY×22 yday $42.86 → 09:30 $42.93 +1.54; CABA×280 yday $3.57 → 09:30 $3.63 +16.80; VSTM×118 yday $8.02 → 09:30 $8.03 +1.18; RVTY×7 yday $130.94 → 09:30 $132.45 +10.57; GPRO×750 yday $1.69 → 09:30 $1.78 +67.50; FRVO×49 yday $17.98 → 09:30 $18.27 +14.21; CRK×58 yday $15.54 → 09:30 $15.45 -5.22 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 1 | $10.41 | $0.11 | — | $80.09 | — | 40% to #1, rest split; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $13.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 6 | $1.95 | $0.14 | — | $68.26 | — | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $13.59 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 64.23 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
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
| 2026-08-17 | `EOG` | cash | leftover split 10.67 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.67 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.67 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AU` | cash | leftover split 42.62 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 9.13 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 9.13 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.13 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 9.13 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-27 | `CRK` | cash | leftover split 11.28 < 1 share @ 14.09 |
| 2026-08-27 | `ACMR` | cash | leftover split 11.28 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 11.28 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 11.28 < 1 share @ 925.74 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `ASND` | cash | leftover split 36.24 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 13.59 < 1 share @ 30.65 |
| 2026-09-04 | `BVS` | cash | leftover split 13.59 < 1 share @ 14.50 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 85 | 2026-09-03 @ $49.76 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+10.6; leftover $4275.30 |
| `HRMY` | 22 | 2026-09-03 @ $41.31 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $916.14 |
| `CABA` | 280 | 2026-09-03 @ $3.27 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+13.8; leftover $916.14 |
| `VSTM` | 118 | 2026-09-03 @ $7.70 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $916.14 |
| `RVTY` | 7 | 2026-09-03 @ $125.94 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $916.14 |
| `GPRO` | 750 | 2026-09-03 @ $1.22 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $916.14 |
| `FRVO` | 49 | 2026-09-03 @ $18.40 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $916.14 |
| `CRK` | 58 | 2026-09-03 @ $15.70 | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $916.14 |
| `NVAX` | 1 | 2026-09-04 @ $10.41 | 40% to #1, rest split; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $13.59 |
| `BAK` | 6 | 2026-09-04 @ $1.95 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $13.59 |
