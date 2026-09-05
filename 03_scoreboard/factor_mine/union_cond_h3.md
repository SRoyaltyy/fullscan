# Factor mine action — `union_cond_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · rank by cond

Cash book **+0.40%** ($10,040) · signal-only (no cash/fees) was +4.26%. Starts YES **6/17**. Fills 103 · skips 171 · realized $+1.95.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $75.84.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, HIMS, INO, IREN, SLS, TGTX, TNDM, TPG | — | $97.53 | $10,153.12 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24 | $10,178.12 | +25.00 | BTBT, AIRO, AMPY, ANGX | — | $55.46 | $10,433.94 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24, BTBT×8, AIRO×1, AMPY×2, ANGX×2 | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08 |
| 2026-08-17 | +2.25 | $55.46 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24, BTBT×8, AIRO×1, AMPY×2, ANGX×2 | $10,413.77 | -20.17 | INV, XHG | — | $44.67 | $10,523.35 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24, BTBT×8, AIRO×1, AMPY×2, ANGX×2, INV×4, XHG×1 | 09:30 open · cash $55.46 (unchanged overnight, no fees) · equity $10,413.77 vs prior close $10,433.94 (-20.17) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00; AMPY×2 yday $4.78 → 09:30 $4.86 +0.16; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46 |
| 2026-08-18 | -6.20 | $44.67 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24, BTBT×8, AIRO×1, AMPY×2, ANGX×2, INV×4, XHG×1 | $10,390.40 | -132.95 | — | BTSG, HIMS, INO, IREN, SLS, TGTX, TNDM, TPG | $10,305.28 | $10,354.78 | BTBT×8, AIRO×1, AMPY×2, ANGX×2, INV×4, XHG×1 | 09:30 open · cash $44.67 (unchanged overnight, no fees) · equity $10,390.40 vs prior close $10,523.35 (-132.95) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; AMPY×2 yday $4.82 → 09:30 $4.91 +0.18; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; INV×4 yday $1.39 → 09:30 $1.32 -0.24; XHG×1 yday $3.91 → 09:30 $3.94 +0.03 |
| 2026-08-19 | -7.20 | $10,305.28 | BTBT×8, AIRO×1, AMPY×2, ANGX×2, INV×4, XHG×1 | $10,354.96 | +0.18 | — | BTBT, AIRO, AMPY, ANGX | $10,344.57 | $10,355.06 | INV×4, XHG×1 | 09:30 open · cash $10,305.28 (unchanged overnight, no fees) · equity $10,354.96 vs prior close $10,354.78 (+0.18) because holdings re-marked: BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; AMPY×2 yday $4.82 → 09:30 $4.88 +0.12; ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; INV×4 yday $1.32 → 09:30 $1.39 +0.26; XHG×1 yday $4.28 → 09:30 $4.32 +0.04 |
| 2026-08-20 | +1.12 | $10,344.57 | INV×4, XHG×1 | $10,354.87 | -0.19 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | INV, XHG | $208.95 | $10,569.29 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | 09:30 open · cash $10,344.57 (unchanged overnight, no fees) · equity $10,354.87 vs prior close $10,355.06 (-0.19) because holdings re-marked: INV×4 yday $1.54 → 09:30 $1.55 +0.04; XHG×1 yday $4.33 → 09:30 $4.10 -0.23 |
| 2026-08-21 | +3.25 | $208.95 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | $10,845.18 | +275.89 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $93.35 | $10,844.20 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $208.95 (unchanged overnight, no fees) · equity $10,845.18 vs prior close $10,569.29 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $93.35 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,973.58 | +129.38 | — | — | $93.35 | $10,815.18 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | 09:30 open · cash $93.35 (unchanged overnight, no fees) · equity $10,973.58 vs prior close $10,844.20 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 |
| 2026-08-25 | +1.80 | $93.35 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | $10,885.04 | +69.86 | AU, ERO, FCX, CNH, HMY, MOS, RHI, SUZ | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $87.01 | $10,844.84 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147 | 09:30 open · cash $93.35 (unchanged overnight, no fees) · equity $10,885.04 vs prior close $10,815.18 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 |
| 2026-08-26 | +2.02 | $87.01 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147 | $10,844.84 | +0.00 | — | — | $87.01 | $10,842.28 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147 | 09:30 open · cash $87.01 (unchanged overnight, no fees) · equity $10,844.84 vs prior close $10,844.84 (+0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00; ERO×35 yday $38.55 → 09:30 $38.55 +0.00; FCX×17 yday $77.49 → 09:30 $77.49 +0.00; CNH×114 yday $11.80 → 09:30 $11.80 +0.00; HMY×59 yday $22.50 → 09:30 $22.50 +0.00; MOS×55 yday $23.75 → 09:30 $23.75 +0.00; RHI×30 yday $44.48 → 09:30 $44.48 +0.00; SUZ×147 yday $9.18 → 09:30 $9.18 +0.00 |
| 2026-08-27 | — | $87.01 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147 | $10,959.71 | +117.43 | GGB, SLI | AUPH, ARCT, AUTL, CRDL, CYPH | $158.07 | $10,856.38 | AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147, GGB×6, SLI×11 | 09:30 open · cash $87.01 (unchanged overnight, no fees) · equity $10,959.71 vs prior close $10,842.28 (+117.43) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; AU×11 yday $118.55 → 09:30 $119.80 +13.75; ERO×35 yday $38.55 → 09:30 $40.51 +68.60; FCX×17 yday $77.49 → 09:30 $79.34 +31.45; CNH×114 yday $11.80 → 09:30 $11.54 -29.64; HMY×59 yday $22.50 → 09:30 $22.39 -6.49; MOS×55 yday $23.75 → 09:30 $24.84 +59.95; RHI×30 yday $44.48 → 09:30 $44.33 -4.50; SUZ×147 yday $9.18 → 09:30 $9.03 -22.05 |
| 2026-08-28 | +0.75 | $158.07 | AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147, GGB×6, SLI×11 | $10,721.35 | -135.03 | KEYS, SMTC, CIEN, MPWR, AVT, CGNX, COHR, LSCC | AU, ERO, FCX, CNH, HMY, MOS, RHI, SUZ | $566.57 | $10,566.93 | GGB×6, SLI×11, KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | 09:30 open · cash $158.07 (unchanged overnight, no fees) · equity $10,721.35 vs prior close $10,856.38 (-135.03) because holdings re-marked: AU×11 yday $118.11 → 09:30 $117.41 -7.70; ERO×35 yday $39.24 → 09:30 $39.20 -1.40; FCX×17 yday $79.00 → 09:30 $78.83 -2.89; CNH×114 yday $11.62 → 09:30 $11.62 +0.00; HMY×59 yday $22.43 → 09:30 $20.70 -102.07; MOS×55 yday $24.16 → 09:30 $24.00 -8.80; RHI×30 yday $44.54 → 09:30 $44.41 -3.90; SUZ×147 yday $8.94 → 09:30 $8.88 -8.82; GGB×6 yday $4.46 → 09:30 $4.57 +0.66; SLI×11 yday $2.61 → 09:30 $2.60 -0.11 |
| 2026-08-31 | -5.85 | $566.57 | GGB×6, SLI×11, KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | $10,155.60 | -411.33 | — | — | $566.57 | $10,146.26 | GGB×6, SLI×11, KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | 09:30 open · cash $566.57 (unchanged overnight, no fees) · equity $10,155.60 vs prior close $10,566.93 (-411.33) because holdings re-marked: GGB×6 yday $4.70 → 09:30 $4.55 -0.90; SLI×11 yday $2.64 → 09:30 $2.51 -1.43; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; CIEN×3 yday $399.85 → 09:30 $373.68 -78.51; MPWR×1 yday $1311.08 → 09:30 $1288.35 -22.73; AVT×14 yday $91.51 → 09:30 $88.63 -40.32; CGNX×21 yday $62.97 → 09:30 $60.31 -55.86; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×10 yday $120.47 → 09:30 $116.00 -44.70 |
| 2026-09-01 | -6.30 | $566.57 | GGB×6, SLI×11, KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | $10,191.11 | +44.85 | — | GGB, SLI | $623.26 | $10,096.51 | KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | 09:30 open · cash $566.57 (unchanged overnight, no fees) · equity $10,191.11 vs prior close $10,146.26 (+44.85) because holdings re-marked: GGB×6 yday $4.55 → 09:30 $4.61 +0.36; SLI×11 yday $2.51 → 09:30 $2.70 +2.09; KEYS×4 yday $319.02 → 09:30 $323.71 +18.76; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; CIEN×3 yday $379.87 → 09:30 $383.85 +11.94; MPWR×1 yday $1270.00 → 09:30 $1279.37 +9.37; AVT×14 yday $88.63 → 09:30 $89.90 +17.78; CGNX×21 yday $60.31 → 09:30 $61.00 +14.49; COHR×4 yday $281.26 → 09:30 $277.23 -16.12; LSCC×10 yday $114.64 → 09:30 $113.97 -6.70 |
| 2026-09-02 | -3.83 | $623.26 | KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | $10,018.20 | -78.31 | — | KEYS, SMTC, CIEN, MPWR, AVT, CGNX, COHR, LSCC | $10,001.93 | $10,001.93 | — | 09:30 open · cash $623.26 (unchanged overnight, no fees) · equity $10,018.20 vs prior close $10,096.51 (-78.31) because holdings re-marked: KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; CIEN×3 yday $378.12 → 09:30 $376.89 -3.69; MPWR×1 yday $1253.54 → 09:30 $1245.11 -8.43; AVT×14 yday $89.90 → 09:30 $88.58 -18.48; CGNX×21 yday $60.57 → 09:30 $59.72 -17.85; COHR×4 yday $272.07 → 09:30 $270.50 -6.28; LSCC×10 yday $113.97 → 09:30 $113.60 -3.70 |
| 2026-09-03 | -0.90 | $10,001.93 | — | $10,001.93 | -0.00 | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | — | $162.37 | $10,188.88 | ARCT×75, BMEA×694, CRDL×578, HRMY×30, NVAX×121, PBH×23, PCRX×47, RVTY×9 | 09:30 open · cash $10,001.93 · no holdings · equity $10,001.93 vs prior close $10,001.93 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $162.37 | ARCT×75, BMEA×694, CRDL×578, HRMY×30, NVAX×121, PBH×23, PCRX×47, RVTY×9 | $10,245.27 | +56.39 | CABA, ALEC, OABI, OPK, BVS | — | $75.84 | $10,039.84 | ARCT×75, BMEA×694, CRDL×578, HRMY×30, NVAX×121, PBH×23, PCRX×47, RVTY×9, CABA×5, ALEC×7, OABI×3, OPK×11, BVS×1 | 09:30 open · cash $162.37 (unchanged overnight, no fees) · equity $10,245.27 vs prior close $10,188.88 (+56.39) because holdings re-marked: ARCT×75 yday $16.74 → 09:30 $16.77 +2.25; BMEA×694 yday $1.93 → 09:30 $1.93 +0.00; CRDL×578 yday $2.17 → 09:30 $2.18 +5.78; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; NVAX×121 yday $10.32 → 09:30 $10.41 +10.89; PBH×23 yday $52.83 → 09:30 $53.45 +14.26; PCRX×47 yday $26.58 → 09:30 $26.74 +7.52; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $7,550.75 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $6,283.80 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $5,040.27 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,797.76 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $2,553.19 | — | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $1,314.55 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $97.53 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $97.53 | ▲ 09:30 equity $10,178.12 vs yday $10,153.12 (+25.00) | 09:30 open · cash $97.53 (unchanged overnight, no fees) · equity $10,178.12 vs prior close $10,153.12 (+25.00) because holdings re-marked: BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; TGTX×25 yday $47.94 → 09:30 $47.27 -16.75; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08 | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $85.39 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $74.16 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 2 | $4.94 | $0.10 | — | $64.17 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $55.46 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.46 | ▼ 09:30 equity $10,413.77 vs yday $10,433.94 (-20.17) | 09:30 open · cash $55.46 (unchanged overnight, no fees) · equity $10,413.77 vs prior close $10,433.94 (-20.17) because holdings re-marked: BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; TGTX×25 yday $48.74 → 09:30 $48.74 +0.00; TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; BTBT×8 yday $1.57 → 09:30 $1.52 -0.40; AIRO×1 yday $9.57 → 09:30 $9.57 +0.00; AMPY×2 yday $4.78 → 09:30 $4.86 +0.16; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46 | — |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 4 | $1.62 | $0.08 | — | $48.90 | — | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $6.93 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 1 | $4.19 | $0.04 | — | $44.67 | — | rank by cond; rank cond; list yday_mover; ⚪; ret5=+291.8; leftover $6.93 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $44.67 | ▼ 09:30 equity $10,390.40 vs yday $10,523.35 (-132.95) | 09:30 open · cash $44.67 (unchanged overnight, no fees) · equity $10,390.40 vs prior close $10,523.35 (-132.95) because holdings re-marked: BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; TGTX×25 yday $49.28 → 09:30 $49.28 +0.00; TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; BTBT×8 yday $1.60 → 09:30 $1.54 -0.48; AIRO×1 yday $9.41 → 09:30 $9.01 -0.40; AMPY×2 yday $4.82 → 09:30 $4.91 +0.18; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; INV×4 yday $1.39 → 09:30 $1.32 -0.24; XHG×1 yday $3.91 → 09:30 $3.94 +0.03 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,242.60 | ▼ -0.12 after sell → book $10,388.33; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $2,410.16 | ▼ -83.63 after sell → book $10,386.19; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $4,149.01 | ▲ +471.89 after sell → book $10,366.02; vs 09:30 mark -20.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $5,323.04 | ▼ -69.50 after sell → book $10,363.93; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,662.66 | ▲ +97.12 after sell → book $10,361.59; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $7,892.57 | ▼ -14.65 after sell → book $10,359.50; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $9,064.89 | ▼ -66.33 after sell → book $10,357.34; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $10,305.28 | ▲ +23.38 after sell → book $10,355.25; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,305.28 | ▲ 09:30 equity $10,354.96 vs yday $10,354.78 (+0.18) | 09:30 open · cash $10,305.28 (unchanged overnight, no fees) · equity $10,354.96 vs prior close $10,354.78 (+0.18) because holdings re-marked: BTBT×8 yday $1.45 → 09:30 $1.42 -0.24; AIRO×1 yday $8.98 → 09:30 $9.10 +0.12; AMPY×2 yday $4.82 → 09:30 $4.88 +0.12; ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; INV×4 yday $1.32 → 09:30 $1.39 +0.26; XHG×1 yday $4.28 → 09:30 $4.32 +0.04 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,316.49 | ▼ -0.94 after sell → book $10,354.81; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,325.47 | ▼ -2.25 after sell → book $10,354.69; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AMPY` | 2 | $4.88 | $0.12 | $-0.35 | $10,335.11 | ▼ -0.35 after sell → book $10,354.57; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $10,344.57 | ▲ +0.75 after sell → book $10,354.45; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,344.57 | ▼ 09:30 equity $10,354.87 vs yday $10,355.06 (-0.19) | 09:30 open · cash $10,344.57 (unchanged overnight, no fees) · equity $10,354.87 vs prior close $10,355.06 (-0.19) because holdings re-marked: INV×4 yday $1.54 → 09:30 $1.55 +0.04; XHG×1 yday $4.33 → 09:30 $4.10 -0.23 | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 4 | $1.55 | $0.09 | $-0.45 | $10,350.67 | ▼ -0.45 after sell → book $10,354.77; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 1 | $4.10 | $0.06 | $-0.20 | $10,354.71 | ▼ -0.20 after sell → book $10,354.71; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,078.43 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.26 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,519.78 | — | rank by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,224.42 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.28 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.07 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.29 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.95 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $208.95 | ▲ 09:30 equity $10,845.18 vs yday $10,569.29 (+275.89) | 09:30 open · cash $208.95 (unchanged overnight, no fees) · equity $10,845.18 vs prior close $10,569.29 (+275.89) because holdings re-marked: AG×62 yday $21.19 → 09:30 $21.90 +44.02; BHP×14 yday $93.63 → 09:30 $95.72 +29.26; CDE×62 yday $21.11 → 09:30 $21.75 +39.68; HDSN×224 yday $5.57 → 09:30 $5.67 +22.40; IAG×65 yday $20.50 → 09:30 $21.17 +43.55; KGC×43 yday $31.43 → 09:30 $32.17 +31.82; NFGC×739 yday $1.75 → 09:30 $1.79 +29.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $191.58 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.09 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.11 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $118.73 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $93.35 | — | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.35 | ▲ 09:30 equity $10,973.58 vs yday $10,844.20 (+129.38) | 09:30 open · cash $93.35 (unchanged overnight, no fees) · equity $10,973.58 vs prior close $10,844.20 (+129.38) because holdings re-marked: AG×62 yday $21.09 → 09:30 $21.47 +23.56; BHP×14 yday $97.03 → 09:30 $97.34 +4.34; CDE×62 yday $20.97 → 09:30 $21.26 +17.98; HDSN×224 yday $5.63 → 09:30 $5.69 +13.44; IAG×65 yday $21.14 → 09:30 $21.44 +19.50; KGC×43 yday $32.76 → 09:30 $33.21 +19.35; NFGC×739 yday $1.84 → 09:30 $1.86 +14.78; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×2 yday $13.45 → 09:30 $13.26 -0.38; AUTL×10 yday $2.41 → 09:30 $2.36 -0.50; CRDL×13 yday $1.86 → 09:30 $1.87 +0.13; CYPH×19 yday $1.42 → 09:30 $1.83 +7.79 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.35 | ▲ 09:30 equity $10,885.04 vs yday $10,815.18 (+69.86) | 09:30 open · cash $93.35 (unchanged overnight, no fees) · equity $10,885.04 vs prior close $10,815.18 (+69.86) because holdings re-marked: AG×62 yday $20.57 → 09:30 $20.73 +9.92; BHP×14 yday $96.66 → 09:30 $95.95 -9.94; CDE×62 yday $20.49 → 09:30 $20.85 +22.32; HDSN×224 yday $5.57 → 09:30 $5.53 -8.96; IAG×65 yday $21.36 → 09:30 $21.63 +17.55; KGC×43 yday $32.47 → 09:30 $32.76 +12.47; NFGC×739 yday $1.90 → 09:30 $1.91 +7.39; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×2 yday $13.76 → 09:30 $14.34 +1.16; AUTL×10 yday $2.38 → 09:30 $2.32 -0.60; CRDL×13 yday $1.80 → 09:30 $1.90 +1.30; CYPH×19 yday $1.64 → 09:30 $1.70 +1.14 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,376.41 | ▲ +6.79 after sell → book $10,882.84; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,717.66 | ▲ +65.08 after sell → book $10,880.79; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.85 | $2.20 | $+8.03 | $4,008.16 | ▲ +8.03 after sell → book $10,878.59; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,243.94 | ▼ -59.59 after sell → book $10,875.65; vs 09:30 mark -2.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $6,647.69 | ▲ +125.61 after sell → book $10,873.45; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $8,054.23 | ▲ +130.33 after sell → book $10,871.31; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.91 | $9.67 | $+99.04 | $9,456.05 | ▲ +99.04 after sell → book $10,861.64; vs 09:30 mark -9.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,734.01 | ▲ +119.63 after sell → book $10,859.60; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $119.46 | $2.02 | — | $9,417.93 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 35 | $38.00 | $2.10 | — | $8,085.84 | — | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.90 | $2.04 | — | $6,759.50 | — | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 114 | $11.72 | $2.33 | — | $5,421.08 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1341.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 59 | $22.65 | $2.17 | — | $4,082.57 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+21.1; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $2,760.41 | — | rank by cond; rank cond; list flatten; ⚪; ret5=+13.0; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $44.52 | $2.08 | — | $1,422.73 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+3.5; leftover $1341.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 147 | $9.07 | $2.43 | — | $87.01 | — | rank by cond; rank cond; list mover_buy; ⚪; ret5=+8.3; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.01 | ▲ 09:30 equity $10,844.84 vs yday $10,844.84 (+0.00) | 09:30 open · cash $87.01 (unchanged overnight, no fees) · equity $10,844.84 vs prior close $10,844.84 (+0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×2 yday $14.21 → 09:30 $14.21 +0.00; AUTL×10 yday $2.34 → 09:30 $2.34 +0.00; CRDL×13 yday $1.90 → 09:30 $1.90 +0.00; CYPH×19 yday $1.64 → 09:30 $1.64 +0.00; AU×11 yday $118.55 → 09:30 $118.55 +0.00; ERO×35 yday $38.55 → 09:30 $38.55 +0.00; FCX×17 yday $77.49 → 09:30 $77.49 +0.00; CNH×114 yday $11.80 → 09:30 $11.80 +0.00; HMY×59 yday $22.50 → 09:30 $22.50 +0.00; MOS×55 yday $23.75 → 09:30 $23.75 +0.00; RHI×30 yday $44.48 → 09:30 $44.48 +0.00; SUZ×147 yday $9.18 → 09:30 $9.18 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.01 | ▲ 09:30 equity $10,959.71 vs yday $10,842.28 (+117.43) | 09:30 open · cash $87.01 (unchanged overnight, no fees) · equity $10,959.71 vs prior close $10,842.28 (+117.43) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×2 yday $14.21 → 09:30 $15.35 +2.28; AUTL×10 yday $2.34 → 09:30 $2.41 +0.70; CRDL×13 yday $1.90 → 09:30 $2.03 +1.69; CYPH×19 yday $1.64 → 09:30 $1.60 -0.76; AU×11 yday $118.55 → 09:30 $119.80 +13.75; ERO×35 yday $38.55 → 09:30 $40.51 +68.60; FCX×17 yday $77.49 → 09:30 $79.34 +31.45; CNH×114 yday $11.80 → 09:30 $11.54 -29.64; HMY×59 yday $22.50 → 09:30 $22.39 -6.49; MOS×55 yday $23.75 → 09:30 $24.84 +59.95; RHI×30 yday $44.48 → 09:30 $44.33 -4.50; SUZ×147 yday $9.18 → 09:30 $9.03 -22.05 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $103.42 | ▼ -0.96 after sell → book $10,959.52; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $133.79 | ▲ +7.88 after sell → book $10,959.19; vs 09:30 mark -0.33 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $157.60 | ▼ -1.17 after sell → book $10,958.90; vs 09:30 mark -0.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $183.66 | ▲ +0.69 after sell → book $10,958.57; vs 09:30 mark -0.33 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $213.68 | ▲ +4.63 after sell → book $10,958.19; vs 09:30 mark -0.38 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 6 | $4.42 | $0.28 | — | $186.88 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=-8.6; leftover $30.53 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 11 | $2.59 | $0.32 | — | $158.07 | — | rank by cond; rank cond; list flatten; ret5=+4.2; leftover $30.53 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.07 | ▼ 09:30 equity $10,721.35 vs yday $10,856.38 (-135.03) | 09:30 open · cash $158.07 (unchanged overnight, no fees) · equity $10,721.35 vs prior close $10,856.38 (-135.03) because holdings re-marked: AU×11 yday $118.11 → 09:30 $117.41 -7.70; ERO×35 yday $39.24 → 09:30 $39.20 -1.40; FCX×17 yday $79.00 → 09:30 $78.83 -2.89; CNH×114 yday $11.62 → 09:30 $11.62 +0.00; HMY×59 yday $22.43 → 09:30 $20.70 -102.07; MOS×55 yday $24.16 → 09:30 $24.00 -8.80; RHI×30 yday $44.54 → 09:30 $44.41 -3.90; SUZ×147 yday $8.94 → 09:30 $8.88 -8.82; GGB×6 yday $4.46 → 09:30 $4.57 +0.66; SLI×11 yday $2.61 → 09:30 $2.60 -0.11 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $117.41 | $2.04 | $-26.62 | $1,447.54 | ▼ -26.62 after sell → book $10,719.31; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 35 | $39.20 | $2.12 | $+37.79 | $2,817.42 | ▲ +37.79 after sell → book $10,717.19; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.83 | $2.06 | $+11.71 | $4,155.47 | ▲ +11.71 after sell → book $10,715.13; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 114 | $11.62 | $2.36 | $-16.09 | $5,477.79 | ▼ -16.09 after sell → book $10,712.77; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HMY` | 59 | $20.70 | $2.19 | $-119.40 | $6,696.90 | ▼ -119.40 after sell → book $10,710.58; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $24.00 | $2.18 | $-4.33 | $8,014.73 | ▼ -4.33 after sell → book $10,708.41; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 30 | $44.41 | $2.10 | $-7.48 | $9,344.93 | ▼ -7.48 after sell → book $10,706.31; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUZ` | 147 | $8.88 | $2.47 | $-32.83 | $10,647.82 | ▼ -32.83 after sell → book $10,703.84; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $9,350.54 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $8,153.32 | — | rank by cond; rank cond; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $411.53 | $2.00 | — | $6,916.74 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=-7.7; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1319.75 | $1.99 | — | $5,594.99 | — | rank by cond; rank cond; list mover_buy; 🔵; ret5=-6.1; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $4,317.42 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.80 | $2.05 | — | $2,996.57 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $1,779.89 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $121.13 | $2.02 | — | $566.57 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $566.57 | ▼ 09:30 equity $10,155.60 vs yday $10,566.93 (-411.33) | 09:30 open · cash $566.57 (unchanged overnight, no fees) · equity $10,155.60 vs prior close $10,566.93 (-411.33) because holdings re-marked: GGB×6 yday $4.70 → 09:30 $4.55 -0.90; SLI×11 yday $2.64 → 09:30 $2.51 -1.43; KEYS×4 yday $325.82 → 09:30 $324.14 -6.72; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; CIEN×3 yday $399.85 → 09:30 $373.68 -78.51; MPWR×1 yday $1311.08 → 09:30 $1288.35 -22.73; AVT×14 yday $91.51 → 09:30 $88.63 -40.32; CGNX×21 yday $62.97 → 09:30 $60.31 -55.86; COHR×4 yday $295.39 → 09:30 $274.13 -85.04; LSCC×10 yday $120.47 → 09:30 $116.00 -44.70 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $566.57 | ▲ 09:30 equity $10,191.11 vs yday $10,146.26 (+44.85) | 09:30 open · cash $566.57 (unchanged overnight, no fees) · equity $10,191.11 vs prior close $10,146.26 (+44.85) because holdings re-marked: GGB×6 yday $4.55 → 09:30 $4.61 +0.36; SLI×11 yday $2.51 → 09:30 $2.70 +2.09; KEYS×4 yday $319.02 → 09:30 $323.71 +18.76; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; CIEN×3 yday $379.87 → 09:30 $383.85 +11.94; MPWR×1 yday $1270.00 → 09:30 $1279.37 +9.37; AVT×14 yday $88.63 → 09:30 $89.90 +17.78; CGNX×21 yday $60.31 → 09:30 $61.00 +14.49; COHR×4 yday $281.26 → 09:30 $277.23 -16.12; LSCC×10 yday $114.64 → 09:30 $113.97 -6.70 | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 6 | $4.61 | $0.31 | $+0.54 | $593.91 | ▲ +0.54 after sell → book $10,190.79; vs 09:30 mark -0.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 11 | $2.70 | $0.35 | $+0.54 | $623.26 | ▲ +0.54 after sell → book $10,190.44; vs 09:30 mark -0.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $623.26 | ▼ 09:30 equity $10,018.20 vs yday $10,096.51 (-78.31) | 09:30 open · cash $623.26 (unchanged overnight, no fees) · equity $10,018.20 vs prior close $10,096.51 (-78.31) because holdings re-marked: KEYS×4 yday $322.70 → 09:30 $321.47 -4.92; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; CIEN×3 yday $378.12 → 09:30 $376.89 -3.69; MPWR×1 yday $1253.54 → 09:30 $1245.11 -8.43; AVT×14 yday $89.90 → 09:30 $88.58 -18.48; CGNX×21 yday $60.57 → 09:30 $59.72 -17.85; COHR×4 yday $272.07 → 09:30 $270.50 -6.28; LSCC×10 yday $113.97 → 09:30 $113.60 -3.70 | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $321.47 | $2.02 | $-13.42 | $1,907.12 | ▼ -13.42 after sell → book $10,016.18; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $2,926.12 | ▼ -178.21 after sell → book $10,014.14; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $376.89 | $2.02 | $-107.94 | $4,054.78 | ▼ -107.94 after sell → book $10,012.13; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1245.11 | $2.01 | $-78.65 | $5,297.87 | ▼ -78.65 after sell → book $10,010.11; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $88.58 | $2.05 | $-39.50 | $6,535.94 | ▼ -39.50 after sell → book $10,008.06; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $59.72 | $2.07 | $-68.81 | $7,787.99 | ▼ -68.81 after sell → book $10,005.99; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $270.50 | $2.02 | $-136.70 | $8,867.97 | ▼ -136.70 after sell → book $10,003.97; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 10 | $113.60 | $2.04 | $-79.36 | $10,001.93 | ▼ -79.36 after sell → book $10,001.93; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,001.93 | ▲ 09:30 equity $10,001.93 vs yday $10,001.93 (-0.00) | 09:30 open · cash $10,001.93 · no holdings · equity $10,001.93 vs prior close $10,001.93 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.46 | $2.21 | — | $8,765.21 | — | rank by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 694 | $1.80 | $8.95 | — | $7,507.06 | — | rank by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 578 | $2.16 | $7.46 | — | $6,251.12 | — | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $5,009.74 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 121 | $10.27 | $2.35 | — | $3,764.72 | — | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $52.88 | $2.06 | — | $2,546.42 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-0.1; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 47 | $26.52 | $2.13 | — | $1,297.85 | — | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $162.37 | — | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $162.37 | ▲ 09:30 equity $10,245.27 vs yday $10,188.88 (+56.39) | 09:30 open · cash $162.37 (unchanged overnight, no fees) · equity $10,245.27 vs prior close $10,188.88 (+56.39) because holdings re-marked: ARCT×75 yday $16.74 → 09:30 $16.77 +2.25; BMEA×694 yday $1.93 → 09:30 $1.93 +0.00; CRDL×578 yday $2.17 → 09:30 $2.18 +5.78; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; NVAX×121 yday $10.32 → 09:30 $10.41 +10.89; PBH×23 yday $52.83 → 09:30 $53.45 +14.26; PCRX×47 yday $26.58 → 09:30 $26.74 +7.52; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 5 | $3.63 | $0.20 | — | $144.03 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $20.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 7 | $2.70 | $0.21 | — | $124.92 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $20.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $5.08 | $0.16 | — | $109.51 | — | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $20.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 11 | $1.71 | $0.22 | — | $90.48 | — | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $20.30 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $75.84 | — | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+0.8; leftover $20.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ARX` | cash | leftover split 12.19 < 1 share @ 19.57 |
| 2026-08-14 | `BETR` | cash | leftover split 12.19 < 1 share @ 14.80 |
| 2026-08-14 | `FIGR` | cash | leftover split 12.19 < 1 share @ 32.12 |
| 2026-08-14 | `ADUR` | cash | leftover split 12.19 < 1 share @ 16.50 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AMPY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ABX` | cash | leftover split 6.93 < 1 share @ 9.12 |
| 2026-08-17 | `NU` | cash | leftover split 6.93 < 1 share @ 15.40 |
| 2026-08-17 | `DVN` | cash | leftover split 6.93 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 6.93 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 6.93 < 1 share @ 202.70 |
| 2026-08-17 | `ALOY` | cash | leftover split 6.93 < 1 share @ 14.66 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AMPY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `XHG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `XHG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EBAY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NOK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TME` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 26.12 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 26.12 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 26.12 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CNH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `HMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RHI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AEM` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-26 | `SCCO` | no_price | no 09:30 open |
| 2026-08-26 | `WPM` | no_price | no 09:30 open |
| 2026-08-26 | `SSRM` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FCX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CNH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HMY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RHI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 30.53 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 30.53 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 30.53 < 1 share @ 925.74 |
| 2026-08-27 | `TX` | cash | leftover split 30.53 < 1 share @ 55.20 |
| 2026-08-27 | `ANET` | cash | leftover split 30.53 < 1 share @ 190.90 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `KEYS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CIEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AVT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CGNX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `COHR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LSCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AVPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CHKP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `KEYS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CIEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `MPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AVT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CGNX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `COHR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LSCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVDA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALAB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ANET` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `APA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CHRD` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CRK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `NVS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ADM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ALVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASND` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ASTH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PBH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `PCRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ATRC` | cash | leftover split 20.30 < 1 share @ 52.88 |
| 2026-09-04 | `MLYS` | cash | leftover split 20.30 < 1 share @ 29.15 |
| 2026-09-04 | `TARS` | cash | leftover split 20.30 < 1 share @ 82.76 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ARCT` | 75 | 2026-09-03 @ $16.46 | rank by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1250.24 |
| `BMEA` | 694 | 2026-09-03 @ $1.80 | rank by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1250.24 |
| `CRDL` | 578 | 2026-09-03 @ $2.16 | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1250.24 |
| `HRMY` | 30 | 2026-09-03 @ $41.31 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1250.24 |
| `NVAX` | 121 | 2026-09-03 @ $10.27 | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1250.24 |
| `PBH` | 23 | 2026-09-03 @ $52.88 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-0.1; leftover $1250.24 |
| `PCRX` | 47 | 2026-09-03 @ $26.52 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1250.24 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1250.24 |
| `CABA` | 5 | 2026-09-04 @ $3.63 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $20.30 |
| `ALEC` | 7 | 2026-09-04 @ $2.70 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $20.30 |
| `OABI` | 3 | 2026-09-04 @ $5.08 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $20.30 |
| `OPK` | 11 | 2026-09-04 @ $1.71 | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $20.30 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+0.8; leftover $20.30 |
