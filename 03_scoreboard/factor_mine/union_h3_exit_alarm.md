# Factor mine action — `union_h3_exit_alarm`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hold 3d, sell next 09:30 if 🚨

Cash book **+8.38%** ($10,838) · signal-only (no cash/fees) was +26.70%. Starts YES **16/17**. Fills 105 · skips 154 · realized $+571.64.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $61.98.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | $10,178.12 | +25.00 | MARA, LDI, BTBT | — | $63.95 | $10,435.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 |
| 2026-08-17 | +2.25 | $63.95 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | $10,414.78 | -20.64 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | INO | $127.33 | $10,364.63 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 |
| 2026-08-18 | -6.20 | $127.33 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | $10,233.31 | -131.32 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, TNDM | $8,649.10 | $10,219.20 | MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | 09:30 open · cash $127.33 (unchanged overnight, no fees) · equity $10,233.31 vs prior close $10,364.63 (-131.32) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; DVN×4 yday $47.57 → 09:30 $48.00 +1.72; EOG×1 yday $146.15 → 09:30 $148.04 +1.89; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; TMC×52 yday $3.77 → 09:30 $3.72 -2.60; TGB×25 yday $8.77 → 09:30 $8.55 -5.50; ELF×2 yday $93.66 → 09:30 $93.44 -0.44; DNN×65 yday $3.19 → 09:30 $3.11 -5.20; NB×41 yday $4.81 → 09:30 $4.66 -6.15 |
| 2026-08-19 | -7.20 | $8,649.10 | MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | $10,244.20 | +25.00 | — | MARA, LDI, BTBT | $8,680.37 | $10,240.81 | DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | 09:30 open · cash $8,649.10 (unchanged overnight, no fees) · equity $10,244.20 vs prior close $10,219.20 (+25.00) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; DVN×4 yday $47.83 → 09:30 $48.22 +1.56; EOG×1 yday $148.70 → 09:30 $149.86 +1.16; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; TMC×52 yday $3.92 → 09:30 $3.93 +0.52; TGB×25 yday $8.36 → 09:30 $8.70 +8.50; ELF×2 yday $92.51 → 09:30 $96.00 +6.98; DNN×65 yday $3.15 → 09:30 $3.19 +2.60; NB×41 yday $4.53 → 09:30 $4.60 +2.87 |
| 2026-08-20 | +1.12 | $8,680.37 | DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | $10,240.75 | -0.06 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | $132.91 | $10,439.68 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, WPM×8 | 09:30 open · cash $8,680.37 (unchanged overnight, no fees) · equity $10,240.75 vs prior close $10,240.81 (-0.06) because holdings re-marked: DVN×4 yday $48.19 → 09:30 $49.02 +3.32; EOG×1 yday $149.48 → 09:30 $151.45 +1.97; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; TMC×52 yday $3.97 → 09:30 $3.92 -2.60; TGB×25 yday $8.47 → 09:30 $8.35 -3.00; ELF×2 yday $99.65 → 09:30 $98.15 -3.00; DNN×65 yday $3.22 → 09:30 $3.20 -1.30; NB×41 yday $4.46 → 09:30 $4.45 -0.41 |
| 2026-08-21 | +3.25 | $132.91 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, WPM×8 | $10,714.27 | +274.59 | ARCT, AUTL, CRDL, CYPH | — | $75.02 | $10,712.47 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, WPM×8, ARCT×1, AUTL×6, CRDL×8, CYPH×12 | 09:30 open · cash $132.91 (unchanged overnight, no fees) · equity $10,714.27 vs prior close $10,439.68 (+274.59) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×221 yday $5.57 → 09:30 $5.67 +22.10; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×730 yday $1.75 → 09:30 $1.79 +29.20; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $75.02 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, WPM×8, ARCT×1, AUTL×6, CRDL×8, CYPH×12 | $10,838.72 | +126.25 | — | WPM, ARCT | $1,357.77 | $10,687.18 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, AUTL×6, CRDL×8, CYPH×12 | 09:30 open · cash $75.02 (unchanged overnight, no fees) · equity $10,838.72 vs prior close $10,712.47 (+126.25) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×61 yday $20.97 → 09:30 $21.26 +17.69; HDSN×221 yday $5.63 → 09:30 $5.69 +13.26; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×730 yday $1.84 → 09:30 $1.86 +14.60; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×6 yday $2.41 → 09:30 $2.36 -0.30; CRDL×8 yday $1.86 → 09:30 $1.87 +0.08; CYPH×12 yday $1.42 → 09:30 $1.83 +4.92 |
| 2026-08-25 | +1.80 | $1,357.77 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, AUTL×6, CRDL×8, CYPH×12 | $10,738.76 | +51.58 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC | $76.53 | $10,685.79 | AUTL×6, CRDL×8, CYPH×12, MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666 | 09:30 open · cash $1,357.77 (unchanged overnight, no fees) · equity $10,738.76 vs prior close $10,687.18 (+51.58) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×61 yday $20.49 → 09:30 $20.85 +21.96; HDSN×221 yday $5.57 → 09:30 $5.53 -8.84; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×730 yday $1.90 → 09:30 $1.91 +7.30; AUTL×6 yday $2.38 → 09:30 $2.32 -0.36; CRDL×8 yday $1.80 → 09:30 $1.90 +0.80; CYPH×12 yday $1.64 → 09:30 $1.70 +0.72 |
| 2026-08-26 | +2.02 | $76.53 | AUTL×6, CRDL×8, CYPH×12, MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666 | $10,685.79 | -0.00 | — | — | $76.53 | $10,682.02 | AUTL×6, CRDL×8, CYPH×12, MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666 | 09:30 open · cash $76.53 (unchanged overnight, no fees) · equity $10,685.79 vs prior close $10,685.79 (-0.00) because holdings re-marked: AUTL×6 yday $2.34 → 09:30 $2.34 +0.00; CRDL×8 yday $1.90 → 09:30 $1.90 +0.00; CYPH×12 yday $1.64 → 09:30 $1.64 +0.00; MOS×55 yday $23.75 → 09:30 $23.75 +0.00; OCUL×122 yday $10.92 → 09:30 $10.92 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×161 yday $8.28 → 09:30 $8.28 +0.00; RZLT×254 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×822 yday $1.61 → 09:30 $1.61 +0.00; NPWR×666 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $76.53 | AUTL×6, CRDL×8, CYPH×12, MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666 | $10,734.00 | +51.98 | CRK, SLI, GGB | AUTL, CRDL, CYPH | $77.98 | $10,590.03 | MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666, CRK×1, SLI×6, GGB×4 | 09:30 open · cash $76.53 (unchanged overnight, no fees) · equity $10,734.00 vs prior close $10,682.02 (+51.98) because holdings re-marked: AUTL×6 yday $2.34 → 09:30 $2.41 +0.42; CRDL×8 yday $1.90 → 09:30 $2.03 +1.04; CYPH×12 yday $1.64 → 09:30 $1.60 -0.48; MOS×55 yday $23.75 → 09:30 $24.84 +59.95; OCUL×122 yday $10.92 → 09:30 $10.79 -15.86; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×161 yday $8.28 → 09:30 $8.60 +51.52; RZLT×254 yday $5.29 → 09:30 $5.01 -71.12; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×822 yday $1.61 → 09:30 $1.75 +115.08; NPWR×666 yday $2.02 → 09:30 $1.93 -59.94 |
| 2026-08-28 | +0.75 | $77.98 | MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666, CRK×1, SLI×6, GGB×4 | $10,624.80 | +34.77 | RRC, ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $153.51 | $10,625.69 | MOS×55, CRK×1, SLI×6, GGB×4, RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | 09:30 open · cash $77.98 (unchanged overnight, no fees) · equity $10,624.80 vs prior close $10,590.03 (+34.77) because holdings re-marked: MOS×55 yday $24.16 → 09:30 $24.00 -8.80; OCUL×122 yday $10.77 → 09:30 $10.63 -17.08; INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×161 yday $8.39 → 09:30 $8.49 +16.10; RZLT×254 yday $5.04 → 09:30 $5.07 +7.62; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×822 yday $1.71 → 09:30 $1.74 +24.66; NPWR×666 yday $1.81 → 09:30 $1.83 +13.32; CRK×1 yday $14.50 → 09:30 $14.42 -0.08; SLI×6 yday $2.61 → 09:30 $2.60 -0.06; GGB×4 yday $4.46 → 09:30 $4.57 +0.44 |
| 2026-08-31 | -5.85 | $153.51 | MOS×55, CRK×1, SLI×6, GGB×4, RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | $10,427.09 | -198.60 | — | MOS | $1,457.59 | $10,442.34 | CRK×1, SLI×6, GGB×4, RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | 09:30 open · cash $153.51 (unchanged overnight, no fees) · equity $10,427.09 vs prior close $10,625.69 (-198.60) because holdings re-marked: MOS×55 yday $23.76 → 09:30 $23.75 -0.55; CRK×1 yday $14.62 → 09:30 $14.56 -0.06; SLI×6 yday $2.64 → 09:30 $2.51 -0.78; GGB×4 yday $4.70 → 09:30 $4.55 -0.60; RRC×44 yday $41.64 → 09:30 $41.11 -23.32; ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BHVN×108 yday $16.12 → 09:30 $15.44 -73.44; BZ×99 yday $18.00 → 09:30 $17.89 -10.89; CAPR×200 yday $10.06 → 09:30 $9.44 -124.00 |
| 2026-09-01 | -6.30 | $1,457.59 | CRK×1, SLI×6, GGB×4, RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | $10,508.49 | +66.15 | — | CRK, SLI, GGB | $1,505.96 | $10,446.47 | RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | 09:30 open · cash $1,457.59 (unchanged overnight, no fees) · equity $10,508.49 vs prior close $10,442.34 (+66.15) because holdings re-marked: CRK×1 yday $14.51 → 09:30 $14.31 -0.20; SLI×6 yday $2.51 → 09:30 $2.70 +1.14; GGB×4 yday $4.55 → 09:30 $4.61 +0.24; RRC×44 yday $41.78 → 09:30 $41.32 -20.24; ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BHVN×108 yday $15.40 → 09:30 $15.45 +5.40; BZ×99 yday $17.90 → 09:30 $17.37 -52.47; CAPR×200 yday $9.36 → 09:30 $10.43 +214.00 |
| 2026-09-02 | -3.83 | $1,505.96 | RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | $10,583.15 | +136.68 | — | RRC, ANF, BHVN, BZ, CAPR | $10,571.65 | $10,571.65 | — | 09:30 open · cash $1,505.96 (unchanged overnight, no fees) · equity $10,583.15 vs prior close $10,446.47 (+136.68) because holdings re-marked: RRC×44 yday $41.32 → 09:30 $41.94 +27.28; ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BHVN×108 yday $15.45 → 09:30 $15.39 -6.48; BZ×99 yday $17.17 → 09:30 $17.29 +11.88; CAPR×200 yday $10.19 → 09:30 $10.77 +116.00 |
| 2026-09-03 | -0.90 | $10,571.65 | — | $10,571.65 | -0.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $121.34 | $11,352.65 | ATRC×26, HRMY×31, CABA×404, VSTM×171, RVTY×10, GPRO×1083, FRVO×71, CRK×84 | 09:30 open · cash $10,571.65 · no holdings · equity $10,571.65 vs prior close $10,571.65 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $121.34 | ATRC×26, HRMY×31, CABA×404, VSTM×171, RVTY×10, GPRO×1083, FRVO×71, CRK×84 | $11,513.91 | +161.26 | NVAX, BVS, BAK | — | $61.98 | $10,838.25 | ATRC×26, HRMY×31, CABA×404, VSTM×171, RVTY×10, GPRO×1083, FRVO×71, CRK×84, NVAX×2, BVS×1, BAK×12 | 09:30 open · cash $121.34 (unchanged overnight, no fees) · equity $11,513.91 vs prior close $11,352.65 (+161.26) because holdings re-marked: ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×31 yday $42.86 → 09:30 $42.93 +2.17; CABA×404 yday $3.57 → 09:30 $3.63 +24.24; VSTM×171 yday $8.02 → 09:30 $8.03 +1.71; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1083 yday $1.69 → 09:30 $1.78 +97.47; FRVO×71 yday $17.98 → 09:30 $18.27 +20.59; CRK×84 yday $15.54 → 09:30 $15.45 -7.56 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13 | — |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.95 | ▼ 09:30 equity $10,414.78 vs yday $10,435.42 (-20.64) | 09:30 open · cash $63.95 (unchanged overnight, no fees) · equity $10,414.78 vs prior close $10,435.42 (-20.64) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; MARA×1 yday $9.20 → 09:30 $9.22 +0.02; LDI×13 yday $0.90 → 09:30 $0.91 +0.13; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40 | — |
| 2026-08-17 09:30 ET | **SELL** | `INO` | 1543 | $1.07 | $20.17 | $+363.88 | $1,694.78 | ▲ +363.88 after sell → book $10,394.60; vs 09:30 mark -20.18 | exit 🚨 after 2 sess | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $1,508.20 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+6.7; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $1,364.00 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+5.8; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $1,159.31 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+8.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 52 | $4.05 | $2.15 | — | $946.56 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-12.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 25 | $8.46 | $2.06 | — | $733.00 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.4; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $550.10 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=-7.2; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 65 | $3.24 | $2.19 | — | $337.32 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+0.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 41 | $5.07 | $2.11 | — | $127.33 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=-4.7; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $127.33 | ▼ 09:30 equity $10,233.31 vs yday $10,364.63 (-131.32) | 09:30 open · cash $127.33 (unchanged overnight, no fees) · equity $10,233.31 vs prior close $10,364.63 (-131.32) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; MARA×1 yday $9.72 → 09:30 $9.36 -0.36; LDI×13 yday $0.88 → 09:30 $0.87 -0.07; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; DVN×4 yday $47.57 → 09:30 $48.00 +1.72; EOG×1 yday $146.15 → 09:30 $148.04 +1.89; FANG×1 yday $206.29 → 09:30 $208.93 +2.64; TMC×52 yday $3.77 → 09:30 $3.72 -2.60; TGB×25 yday $8.77 → 09:30 $8.55 -5.50; ELF×2 yday $93.66 → 09:30 $93.44 -0.44; DNN×65 yday $3.19 → 09:30 $3.11 -5.20; NB×41 yday $4.81 → 09:30 $4.66 -6.15 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,325.26 | ▼ -0.12 after sell → book $10,231.24; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,499.29 | ▼ -69.50 after sell → book $10,229.15; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,739.69 | ▲ +23.38 after sell → book $10,227.07; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,969.61 | ▼ -14.65 after sell → book $10,224.99; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,309.23 | ▲ +97.12 after sell → book $10,222.65; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,476.79 | ▼ -83.63 after sell → book $10,220.51; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $8,649.10 | ▼ -66.33 after sell → book $10,218.34; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,649.10 | ▲ 09:30 equity $10,244.20 vs yday $10,219.20 (+25.00) | 09:30 open · cash $8,649.10 (unchanged overnight, no fees) · equity $10,244.20 vs prior close $10,219.20 (+25.00) because holdings re-marked: MARA×1 yday $8.96 → 09:30 $8.91 -0.05; LDI×13 yday $0.86 → 09:30 $0.88 +0.29; BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; DVN×4 yday $47.83 → 09:30 $48.22 +1.56; EOG×1 yday $148.70 → 09:30 $149.86 +1.16; FANG×1 yday $210.02 → 09:30 $210.84 +0.82; TMC×52 yday $3.92 → 09:30 $3.93 +0.52; TGB×25 yday $8.36 → 09:30 $8.70 +8.50; ELF×2 yday $92.51 → 09:30 $96.00 +6.98; DNN×65 yday $3.15 → 09:30 $3.19 +2.60; NB×41 yday $4.53 → 09:30 $4.60 +2.87 | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $8,657.90 | ▼ -0.31 after sell → book $10,244.09; vs 09:30 mark -0.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $8,669.17 | ▼ -1.08 after sell → book $10,243.92; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $8,680.37 | ▼ -0.94 after sell → book $10,243.76; vs 09:30 mark -0.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,680.37 | ▼ 09:30 equity $10,240.75 vs yday $10,240.81 (-0.06) | 09:30 open · cash $8,680.37 (unchanged overnight, no fees) · equity $10,240.75 vs prior close $10,240.81 (-0.06) because holdings re-marked: DVN×4 yday $48.19 → 09:30 $49.02 +3.32; EOG×1 yday $149.48 → 09:30 $151.45 +1.97; FANG×1 yday $208.55 → 09:30 $213.51 +4.96; TMC×52 yday $3.97 → 09:30 $3.92 -2.60; TGB×25 yday $8.47 → 09:30 $8.35 -3.00; ELF×2 yday $99.65 → 09:30 $98.15 -3.00; DNN×65 yday $3.22 → 09:30 $3.20 -1.30; NB×41 yday $4.46 → 09:30 $4.45 -0.41 | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 4 | $49.02 | $1.99 | $+7.51 | $8,874.46 | ▲ +7.51 after sell → book $10,238.76; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 1 | $151.45 | $1.54 | $+5.71 | $9,024.37 | ▲ +5.71 after sell → book $10,237.22; vs 09:30 mark -1.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,235.87 | ▲ +6.80 after sell → book $10,235.21; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 52 | $3.92 | $2.17 | $-11.07 | $9,437.54 | ▼ -11.07 after sell → book $10,233.04; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 25 | $8.35 | $2.08 | $-6.90 | $9,644.21 | ▼ -6.90 after sell → book $10,230.96; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ELF` | 2 | $98.15 | $1.99 | $+11.41 | $9,838.52 | ▲ +11.41 after sell → book $10,228.97; vs 09:30 mark -1.99 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 65 | $3.20 | $2.21 | $-6.99 | $10,044.31 | ▼ -6.99 after sell → book $10,226.76; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 41 | $4.45 | $1.97 | $-29.50 | $10,224.79 | ▼ -29.50 after sell → book $10,224.79; vs 09:30 mark -1.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,948.52 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,672.35 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,410.52 | — | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 221 | $5.77 | $2.85 | — | $5,132.50 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,854.37 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,578.16 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 730 | $1.75 | $9.42 | — | $1,291.24 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $132.91 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $132.91 | ▲ 09:30 equity $10,714.27 vs yday $10,439.68 (+274.59) | 09:30 open · cash $132.91 (unchanged overnight, no fees) · equity $10,714.27 vs prior close $10,439.68 (+274.59) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×221 yday $5.57 → 09:30 $5.67 +22.10; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×730 yday $1.75 → 09:30 $1.79 +29.20; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $121.66 | — | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $16.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 6 | $2.47 | $0.17 | — | $106.68 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $16.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 8 | $1.93 | $0.18 | — | $91.06 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $16.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 12 | $1.32 | $0.19 | — | $75.02 | — | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $16.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $75.02 | ▲ 09:30 equity $10,838.72 vs yday $10,712.47 (+126.25) | 09:30 open · cash $75.02 (unchanged overnight, no fees) · equity $10,838.72 vs prior close $10,712.47 (+126.25) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×61 yday $20.97 → 09:30 $21.26 +17.69; HDSN×221 yday $5.63 → 09:30 $5.69 +13.26; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×730 yday $1.84 → 09:30 $1.86 +14.60; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×6 yday $2.41 → 09:30 $2.36 -0.30; CRDL×8 yday $1.86 → 09:30 $1.87 +0.08; CYPH×12 yday $1.42 → 09:30 $1.83 +4.92 | — |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 8 | $158.96 | $2.03 | $+111.31 | $1,344.67 | ▲ +111.31 after sell → book $10,836.69; vs 09:30 mark -2.03 | exit 🚨 after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 1 | $13.26 | $0.16 | $+1.86 | $1,357.77 | ▲ +1.86 after sell → book $10,836.53; vs 09:30 mark -0.16 | exit 🚨 after 1 sess | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,357.77 | ▲ 09:30 equity $10,738.76 vs yday $10,687.18 (+51.58) | 09:30 open · cash $1,357.77 (unchanged overnight, no fees) · equity $10,738.76 vs prior close $10,687.18 (+51.58) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×61 yday $20.49 → 09:30 $20.85 +21.96; HDSN×221 yday $5.57 → 09:30 $5.53 -8.84; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×730 yday $1.90 → 09:30 $1.91 +7.30; AUTL×6 yday $2.38 → 09:30 $2.32 -0.36; CRDL×8 yday $1.80 → 09:30 $1.90 +0.80; CYPH×12 yday $1.64 → 09:30 $1.70 +0.72 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $2,640.84 | ▲ +6.79 after sell → book $10,736.57; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $3,982.09 | ▲ +65.08 after sell → book $10,734.52; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 61 | $20.85 | $2.19 | $+7.83 | $5,251.74 | ▲ +7.83 after sell → book $10,732.32; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 221 | $5.53 | $2.90 | $-58.79 | $6,470.97 | ▼ -58.79 after sell → book $10,729.42; vs 09:30 mark -2.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $7,874.72 | ▲ +125.61 after sell → book $10,727.22; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $9,281.26 | ▲ +130.33 after sell → book $10,725.08; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 730 | $1.91 | $9.55 | $+97.83 | $10,666.01 | ▲ +97.83 after sell → book $10,715.53; vs 09:30 mark -9.55 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $9,343.85 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.0; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 122 | $10.92 | $2.36 | — | $8,009.26 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+10.4; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $6,716.33 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+9.2; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 161 | $8.28 | $2.47 | — | $5,380.78 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 254 | $5.23 | $3.28 | — | $4,049.08 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+10.7; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,759.36 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+6.1; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 822 | $1.62 | $10.60 | — | $1,417.12 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 666 | $2.00 | $8.59 | — | $76.53 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1333.25 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.53 | ▲ 09:30 equity $10,685.79 vs yday $10,685.79 (-0.00) | 09:30 open · cash $76.53 (unchanged overnight, no fees) · equity $10,685.79 vs prior close $10,685.79 (-0.00) because holdings re-marked: AUTL×6 yday $2.34 → 09:30 $2.34 +0.00; CRDL×8 yday $1.90 → 09:30 $1.90 +0.00; CYPH×12 yday $1.64 → 09:30 $1.64 +0.00; MOS×55 yday $23.75 → 09:30 $23.75 +0.00; OCUL×122 yday $10.92 → 09:30 $10.92 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×161 yday $8.28 → 09:30 $8.28 +0.00; RZLT×254 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; BMEA×822 yday $1.61 → 09:30 $1.61 +0.00; NPWR×666 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $76.53 | ▲ 09:30 equity $10,734.00 vs yday $10,682.02 (+51.98) | 09:30 open · cash $76.53 (unchanged overnight, no fees) · equity $10,734.00 vs prior close $10,682.02 (+51.98) because holdings re-marked: AUTL×6 yday $2.34 → 09:30 $2.41 +0.42; CRDL×8 yday $1.90 → 09:30 $2.03 +1.04; CYPH×12 yday $1.64 → 09:30 $1.60 -0.48; MOS×55 yday $23.75 → 09:30 $24.84 +59.95; OCUL×122 yday $10.92 → 09:30 $10.79 -15.86; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×161 yday $8.28 → 09:30 $8.60 +51.52; RZLT×254 yday $5.29 → 09:30 $5.01 -71.12; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; BMEA×822 yday $1.61 → 09:30 $1.75 +115.08; NPWR×666 yday $2.02 → 09:30 $1.93 -59.94 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 6 | $2.41 | $0.18 | $-0.71 | $90.81 | ▼ -0.71 after sell → book $10,733.82; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 8 | $2.03 | $0.21 | $+0.42 | $106.84 | ▲ +0.42 after sell → book $10,733.61; vs 09:30 mark -0.21 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 12 | $1.60 | $0.25 | $+2.92 | $125.79 | ▲ +2.92 after sell → book $10,733.36; vs 09:30 mark -0.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 1 | $14.09 | $0.14 | — | $111.56 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+1.1; leftover $17.97 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 6 | $2.59 | $0.17 | — | $95.85 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+4.2; leftover $17.97 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 4 | $4.42 | $0.19 | — | $77.98 | — | hold 3d, sell next 09:30 if 🚨; list mover_buy; 🔵; ret5=-8.6; leftover $17.97 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $77.98 | ▲ 09:30 equity $10,624.80 vs yday $10,590.03 (+34.77) | 09:30 open · cash $77.98 (unchanged overnight, no fees) · equity $10,624.80 vs prior close $10,590.03 (+34.77) because holdings re-marked: MOS×55 yday $24.16 → 09:30 $24.00 -8.80; OCUL×122 yday $10.77 → 09:30 $10.63 -17.08; INSP×21 yday $61.80 → 09:30 $62.10 +6.30; CRMD×161 yday $8.39 → 09:30 $8.49 +16.10; RZLT×254 yday $5.04 → 09:30 $5.07 +7.62; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; BMEA×822 yday $1.71 → 09:30 $1.74 +24.66; NPWR×666 yday $1.81 → 09:30 $1.83 +13.32; CRK×1 yday $14.50 → 09:30 $14.42 -0.08; SLI×6 yday $2.61 → 09:30 $2.60 -0.06; GGB×4 yday $4.46 → 09:30 $4.57 +0.44 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 122 | $10.63 | $2.39 | $-40.12 | $1,372.45 | ▼ -40.12 after sell → book $10,622.41; vs 09:30 mark -2.39 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $62.10 | $2.07 | $+9.10 | $2,674.48 | ▲ +9.10 after sell → book $10,620.34; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 161 | $8.49 | $2.51 | $+28.83 | $4,038.86 | ▲ +28.83 after sell → book $10,617.83; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 254 | $5.07 | $3.33 | $-47.25 | $5,323.31 | ▼ -47.25 after sell → book $10,614.50; vs 09:30 mark -3.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $6,595.12 | ▼ -17.91 after sell → book $10,612.48; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 822 | $1.74 | $10.75 | $+77.28 | $8,014.65 | ▲ +77.28 after sell → book $10,601.73; vs 09:30 mark -10.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 666 | $1.83 | $8.71 | $-130.52 | $9,224.71 | ▼ -130.52 after sell → book $10,593.01; vs 09:30 mark -8.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 44 | $41.44 | $2.12 | — | $7,399.23 | — | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+1.8; leftover $1844.94 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 12 | $144.70 | $2.03 | — | $5,660.81 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1844.94 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 108 | $16.95 | $2.31 | — | $3,827.89 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1844.94 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 99 | $18.50 | $2.29 | — | $1,994.10 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1844.94 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 200 | $9.19 | $2.59 | — | $153.51 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1844.94 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.51 | ▼ 09:30 equity $10,427.09 vs yday $10,625.69 (-198.60) | 09:30 open · cash $153.51 (unchanged overnight, no fees) · equity $10,427.09 vs prior close $10,625.69 (-198.60) because holdings re-marked: MOS×55 yday $23.76 → 09:30 $23.75 -0.55; CRK×1 yday $14.62 → 09:30 $14.56 -0.06; SLI×6 yday $2.64 → 09:30 $2.51 -0.78; GGB×4 yday $4.70 → 09:30 $4.55 -0.60; RRC×44 yday $41.64 → 09:30 $41.11 -23.32; ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BHVN×108 yday $16.12 → 09:30 $15.44 -73.44; BZ×99 yday $18.00 → 09:30 $17.89 -10.89; CAPR×200 yday $10.06 → 09:30 $9.44 -124.00 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.75 | $2.18 | $-18.08 | $1,457.59 | ▼ -18.08 after sell → book $10,424.92; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,457.59 | ▲ 09:30 equity $10,508.49 vs yday $10,442.34 (+66.15) | 09:30 open · cash $1,457.59 (unchanged overnight, no fees) · equity $10,508.49 vs prior close $10,442.34 (+66.15) because holdings re-marked: CRK×1 yday $14.51 → 09:30 $14.31 -0.20; SLI×6 yday $2.51 → 09:30 $2.70 +1.14; GGB×4 yday $4.55 → 09:30 $4.61 +0.24; RRC×44 yday $41.78 → 09:30 $41.32 -20.24; ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BHVN×108 yday $15.40 → 09:30 $15.45 +5.40; BZ×99 yday $17.90 → 09:30 $17.37 -52.47; CAPR×200 yday $9.36 → 09:30 $10.43 +214.00 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 1 | $14.31 | $0.17 | $-0.09 | $1,471.73 | ▼ -0.09 after sell → book $10,508.32; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 6 | $2.70 | $0.20 | $+0.29 | $1,487.73 | ▲ +0.29 after sell → book $10,508.12; vs 09:30 mark -0.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 4 | $4.61 | $0.22 | $+0.35 | $1,505.96 | ▲ +0.35 after sell → book $10,507.91; vs 09:30 mark -0.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,505.96 | ▲ 09:30 equity $10,583.15 vs yday $10,446.47 (+136.68) | 09:30 open · cash $1,505.96 (unchanged overnight, no fees) · equity $10,583.15 vs prior close $10,446.47 (+136.68) because holdings re-marked: RRC×44 yday $41.32 → 09:30 $41.94 +27.28; ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BHVN×108 yday $15.45 → 09:30 $15.39 -6.48; BZ×99 yday $17.17 → 09:30 $17.29 +11.88; CAPR×200 yday $10.19 → 09:30 $10.77 +116.00 | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 44 | $41.94 | $2.15 | $+17.73 | $3,349.17 | ▲ +17.73 after sell → book $10,581.00; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 12 | $142.00 | $2.05 | $-36.48 | $5,051.12 | ▼ -36.48 after sell → book $10,578.95; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 108 | $15.39 | $2.35 | $-173.14 | $6,710.90 | ▼ -173.14 after sell → book $10,576.61; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 99 | $17.29 | $2.32 | $-124.39 | $8,420.29 | ▼ -124.39 after sell → book $10,574.29; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 200 | $10.77 | $2.64 | $+310.77 | $10,571.65 | ▲ +310.77 after sell → book $10,571.65; vs 09:30 mark -2.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,571.65 | ▲ 09:30 equity $10,571.65 vs yday $10,571.65 (-0.00) | 09:30 open · cash $10,571.65 · no holdings · equity $10,571.65 vs prior close $10,571.65 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $9,275.82 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $41.31 | $2.08 | — | $7,993.13 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 404 | $3.27 | $5.21 | — | $6,666.84 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 171 | $7.70 | $2.50 | — | $5,347.63 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,086.21 | — | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1083 | $1.22 | $13.97 | — | $2,750.98 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1321.46 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 71 | $18.40 | $2.20 | — | $1,442.38 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1321.46 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 84 | $15.70 | $2.24 | — | $121.34 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1321.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.34 | ▲ 09:30 equity $11,513.91 vs yday $11,352.65 (+161.26) | 09:30 open · cash $121.34 (unchanged overnight, no fees) · equity $11,513.91 vs prior close $11,352.65 (+161.26) because holdings re-marked: ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×31 yday $42.86 → 09:30 $42.93 +2.17; CABA×404 yday $3.57 → 09:30 $3.63 +24.24; VSTM×171 yday $8.02 → 09:30 $8.03 +1.71; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1083 yday $1.69 → 09:30 $1.78 +97.47; FRVO×71 yday $17.98 → 09:30 $18.27 +20.59; CRK×84 yday $15.54 → 09:30 $15.45 -7.56 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $100.30 | — | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $24.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $85.65 | — | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.8; leftover $24.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 12 | $1.95 | $0.27 | — | $61.98 | — | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $24.27 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 16.61 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 16.61 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 16.61 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 16.61 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-27 | `RRC` | cash | leftover split 17.97 < 1 share @ 40.72 |
| 2026-08-27 | `ACMR` | cash | leftover split 17.97 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 17.97 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 17.97 < 1 share @ 925.74 |
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
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 24.27 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 24.27 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 26 | 2026-09-03 @ $49.76 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1321.46 |
| `HRMY` | 31 | 2026-09-03 @ $41.31 | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1321.46 |
| `CABA` | 404 | 2026-09-03 @ $3.27 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1321.46 |
| `VSTM` | 171 | 2026-09-03 @ $7.70 | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1321.46 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1321.46 |
| `GPRO` | 1083 | 2026-09-03 @ $1.22 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1321.46 |
| `FRVO` | 71 | 2026-09-03 @ $18.40 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1321.46 |
| `CRK` | 84 | 2026-09-03 @ $15.70 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1321.46 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $24.27 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.8; leftover $24.27 |
| `BAK` | 12 | 2026-09-04 @ $1.95 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $24.27 |
