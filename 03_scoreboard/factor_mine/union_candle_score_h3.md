# Factor mine action — `union_candle_score_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `candle_score` · size `leftover` · sell `list` · S-boost `none` · rank by candle_score

Cash book **+11.72%** ($11,172) · signal-only (no cash/fees) was +23.12%. Starts YES **16/17**. Fills 98 · skips 151 · realized $+1049.66.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `candle_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $36.65.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TNDM, TPG, HIMS, IREN, INO, VOR, BTSG, SLS | — | $107.38 | $10,268.71 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106 | $10,312.70 | +43.99 | SATL, NMAX | — | $85.30 | $10,517.00 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106, SATL×2, NMAX×1 | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; SLS×106 yday $12.36 → 09:30 $12.40 +4.24 |
| 2026-08-17 | +2.25 | $85.30 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106, SATL×2, NMAX×1 | $10,490.01 | -26.99 | NPWR, SMJF, BORR | — | $54.16 | $10,590.50 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106, SATL×2, NMAX×1, NPWR×6, SMJF×1, BORR×2 | 09:30 open · cash $85.30 (unchanged overnight, no fees) · equity $10,490.01 vs prior close $10,517.00 (-26.99) because holdings re-marked: TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; SATL×2 yday $5.80 → 09:30 $5.81 +0.02; NMAX×1 yday $10.87 → 09:30 $10.97 +0.10 |
| 2026-08-18 | -6.20 | $54.16 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106, SATL×2, NMAX×1, NPWR×6, SMJF×1, BORR×2 | $10,446.92 | -143.58 | — | TNDM, TPG, HIMS, IREN, INO, VOR, BTSG, SLS | $10,360.61 | $10,413.16 | SATL×2, NMAX×1, NPWR×6, SMJF×1, BORR×2 | 09:30 open · cash $54.16 (unchanged overnight, no fees) · equity $10,446.92 vs prior close $10,590.50 (-143.58) because holdings re-marked: TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; SATL×2 yday $5.81 → 09:30 $5.50 -0.62; NMAX×1 yday $10.36 → 09:30 $10.31 -0.05; NPWR×6 yday $1.73 → 09:30 $1.70 -0.18; SMJF×1 yday $10.45 → 09:30 $10.45 +0.00; BORR×2 yday $4.50 → 09:30 $4.56 +0.12 |
| 2026-08-19 | -7.20 | $10,360.61 | SATL×2, NMAX×1, NPWR×6, SMJF×1, BORR×2 | $10,413.68 | +0.52 | — | SATL | $10,372.10 | $10,412.55 | NMAX×1, NPWR×6, SMJF×1, BORR×2 | 09:30 open · cash $10,360.61 (unchanged overnight, no fees) · equity $10,413.68 vs prior close $10,413.16 (+0.52) because holdings re-marked: SATL×2 yday $5.74 → 09:30 $5.82 +0.16; NMAX×1 yday $11.43 → 09:30 $11.50 +0.07; NPWR×6 yday $1.65 → 09:30 $1.70 +0.30; SMJF×1 yday $10.88 → 09:30 $10.71 -0.17; BORR×2 yday $4.43 → 09:30 $4.51 +0.16 |
| 2026-08-20 | +1.12 | $10,372.10 | NMAX×1, NPWR×6, SMJF×1, BORR×2 | $10,412.47 | -0.08 | IOND, NBP, IMMX, ABCL, MRNA, ABUS, CYPH, GENB | NMAX, NPWR, SMJF, BORR | $139.14 | $10,198.03 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77 | 09:30 open · cash $10,372.10 (unchanged overnight, no fees) · equity $10,412.47 vs prior close $10,412.55 (-0.08) because holdings re-marked: NMAX×1 yday $10.91 → 09:30 $10.89 -0.02; NPWR×6 yday $1.67 → 09:30 $1.64 -0.18; SMJF×1 yday $10.72 → 09:30 $10.72 +0.00; BORR×2 yday $4.40 → 09:30 $4.46 +0.12 |
| 2026-08-21 | +3.25 | $139.14 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77 | $10,478.53 | +280.50 | IOVA, ARCT | — | $109.55 | $10,760.39 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77, IOVA×2, ARCT×1 | 09:30 open · cash $139.14 (unchanged overnight, no fees) · equity $10,478.53 vs prior close $10,198.03 (+280.50) because holdings re-marked: IOND×19 yday $68.77 → 09:30 $68.41 -6.84; NBP×660 yday $1.91 → 09:30 $1.91 +0.00; IMMX×100 yday $13.16 → 09:30 $13.36 +20.00; ABCL×110 yday $11.57 → 09:30 $11.57 +0.00; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ABUS×264 yday $4.77 → 09:30 $5.20 +113.52; CYPH×1131 yday $1.19 → 09:30 $1.32 +147.03; GENB×77 yday $15.99 → 09:30 $16.10 +8.47 |
| 2026-08-24 | -5.17 | $109.55 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77, IOVA×2, ARCT×1 | $11,196.24 | +435.85 | — | — | $109.55 | $10,830.56 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77, IOVA×2, ARCT×1 | 09:30 open · cash $109.55 (unchanged overnight, no fees) · equity $11,196.24 vs prior close $10,760.39 (+435.85) because holdings re-marked: IOND×19 yday $68.73 → 09:30 $68.72 -0.19; NBP×660 yday $2.00 → 09:30 $2.01 +6.60; IMMX×100 yday $13.66 → 09:30 $13.69 +3.00; ABCL×110 yday $11.32 → 09:30 $10.97 -38.50; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; ABUS×264 yday $5.21 → 09:30 $5.18 -7.92; CYPH×1131 yday $1.42 → 09:30 $1.83 +463.71; GENB×77 yday $16.12 → 09:30 $16.50 +29.26; IOVA×2 yday $8.29 → 09:30 $8.05 -0.48; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19 |
| 2026-08-25 | +1.80 | $109.55 | IOND×19, NBP×660, IMMX×100, ABCL×110, MRNA×8, ABUS×264, CYPH×1131, GENB×77, IOVA×2, ARCT×1 | $11,016.73 | +186.17 | OMER, SG, AVAH, RUM, AU, TRLV, BMNR | IOND, NBP, IMMX, ABCL, MRNA, ABUS, GENB | $121.65 | $10,891.41 | CYPH×1131, IOVA×2, ARCT×1, OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52 | 09:30 open · cash $109.55 (unchanged overnight, no fees) · equity $11,016.73 vs prior close $10,830.56 (+186.17) because holdings re-marked: IOND×19 yday $70.11 → 09:30 $68.27 -34.96; NBP×660 yday $1.87 → 09:30 $1.89 +13.20; IMMX×100 yday $13.35 → 09:30 $13.40 +5.00; ABCL×110 yday $10.52 → 09:30 $10.77 +27.50; MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; ABUS×264 yday $5.20 → 09:30 $5.26 +15.84; CYPH×1131 yday $1.64 → 09:30 $1.70 +67.86; GENB×77 yday $16.76 → 09:30 $17.75 +76.23; IOVA×2 yday $8.22 → 09:30 $8.00 -0.44; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58 |
| 2026-08-26 | +2.02 | $121.65 | CYPH×1131, IOVA×2, ARCT×1, OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52 | $10,891.41 | -0.00 | — | — | $121.65 | $10,977.71 | CYPH×1131, IOVA×2, ARCT×1, OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52 | 09:30 open · cash $121.65 (unchanged overnight, no fees) · equity $10,891.41 vs prior close $10,891.41 (-0.00) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.64 +0.00; IOVA×2 yday $8.08 → 09:30 $8.08 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; OMER×68 yday $19.03 → 09:30 $19.03 +0.00; SG×184 yday $7.00 → 09:30 $7.00 +0.00; AVAH×94 yday $13.70 → 09:30 $13.70 +0.00; RUM×137 yday $9.35 → 09:30 $9.35 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; TRLV×117 yday $11.02 → 09:30 $11.02 +0.00; BMNR×52 yday $24.21 → 09:30 $24.21 +0.00 |
| 2026-08-27 | — | $121.65 | CYPH×1131, IOVA×2, ARCT×1, OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52 | $10,965.27 | -12.44 | RRC, GEN, DLO, MOS, PLTR, SLI, PGY, MT | CYPH, IOVA, ARCT | $158.67 | $10,822.75 | OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52, RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3 | 09:30 open · cash $121.65 (unchanged overnight, no fees) · equity $10,965.27 vs prior close $10,977.71 (-12.44) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.60 -45.24; IOVA×2 yday $8.08 → 09:30 $8.34 +0.52; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; OMER×68 yday $19.03 → 09:30 $18.96 -4.76; SG×184 yday $7.00 → 09:30 $6.95 -9.20; AVAH×94 yday $13.70 → 09:30 $13.65 -4.70; RUM×137 yday $9.35 → 09:30 $10.07 +98.64; AU×10 yday $118.55 → 09:30 $119.80 +12.50; TRLV×117 yday $11.02 → 09:30 $11.22 +23.40; BMNR×52 yday $24.21 → 09:30 $24.24 +1.56 |
| 2026-08-28 | +0.75 | $158.67 | OMER×68, SG×184, AVAH×94, RUM×137, AU×10, TRLV×117, BMNR×52, RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3 | $10,889.89 | +67.14 | ZYME, CLYM, NVAX, VIRT, AMTX, ESTC, FIGR | OMER, SG, AVAH, RUM, AU, BMNR | $137.36 | $10,798.23 | TRLV×117, RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3, ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | 09:30 open · cash $158.67 (unchanged overnight, no fees) · equity $10,889.89 vs prior close $10,822.75 (+67.14) because holdings re-marked: OMER×68 yday $18.22 → 09:30 $18.24 +1.36; SG×184 yday $6.85 → 09:30 $6.87 +3.68; AVAH×94 yday $13.62 → 09:30 $13.62 +0.00; RUM×137 yday $9.38 → 09:30 $9.51 +17.81; AU×10 yday $118.11 → 09:30 $117.41 -7.00; TRLV×117 yday $11.43 → 09:30 $11.38 -5.85; BMNR×52 yday $24.91 → 09:30 $25.91 +52.00; RRC×5 yday $41.55 → 09:30 $41.44 -0.55; GEN×8 yday $29.64 → 09:30 $29.83 +1.52; DLO×15 yday $15.36 → 09:30 $15.33 -0.45; MOS×9 yday $24.16 → 09:30 $24.00 -1.44; PLTR×1 yday $177.50 → 09:30 $178.75 +1.25; SLI×94 yday $2.61 → 09:30 $2.60 -0.94; PGY×11 yday $22.41 → 09:30 $22.93 +5.72; MT×3 yday $74.53 → 09:30 $74.54 +0.03 |
| 2026-08-31 | -5.85 | $137.36 | TRLV×117, RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3, ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | $11,039.17 | +240.94 | — | TRLV | $1,586.96 | $11,061.99 | RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3, ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | 09:30 open · cash $137.36 (unchanged overnight, no fees) · equity $11,039.17 vs prior close $10,798.23 (+240.94) because holdings re-marked: TRLV×117 yday $11.03 → 09:30 $12.41 +161.46; RRC×5 yday $41.64 → 09:30 $41.11 -2.65; GEN×8 yday $30.50 → 09:30 $31.02 +4.16; DLO×15 yday $15.14 → 09:30 $15.01 -1.95; MOS×9 yday $23.76 → 09:30 $23.75 -0.09; PLTR×1 yday $185.93 → 09:30 $184.04 -1.89; SLI×94 yday $2.64 → 09:30 $2.51 -12.22; PGY×11 yday $23.26 → 09:30 $21.51 -19.25; MT×3 yday $74.63 → 09:30 $75.07 +1.32; ZYME×37 yday $29.01 → 09:30 $28.27 -27.38; CLYM×68 yday $15.06 → 09:30 $14.65 -27.88; NVAX×121 yday $9.05 → 09:30 $9.23 +21.78; VIRT×16 yday $67.04 → 09:30 $66.39 -10.40; AMTX×592 yday $1.87 → 09:30 $1.90 +17.76; ESTC×13 yday $83.74 → 09:30 $99.99 +211.25; FIGR×29 yday $38.02 → 09:30 $35.50 -73.08 |
| 2026-09-01 | -6.30 | $1,586.96 | RRC×5, GEN×8, DLO×15, MOS×9, PLTR×1, SLI×94, PGY×11, MT×3, ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | $11,001.43 | -60.56 | — | RRC, GEN, DLO, MOS, PLTR, SLI, PGY, MT | $3,362.13 | $10,947.40 | ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | 09:30 open · cash $1,586.96 (unchanged overnight, no fees) · equity $11,001.43 vs prior close $11,061.99 (-60.56) because holdings re-marked: RRC×5 yday $41.78 → 09:30 $41.32 -2.30; GEN×8 yday $31.02 → 09:30 $30.56 -3.68; DLO×15 yday $15.00 → 09:30 $14.88 -1.80; MOS×9 yday $23.78 → 09:30 $24.00 +1.98; PLTR×1 yday $183.80 → 09:30 $185.52 +1.72; SLI×94 yday $2.51 → 09:30 $2.70 +17.86; PGY×11 yday $21.95 → 09:30 $21.73 -2.42; MT×3 yday $75.06 → 09:30 $74.31 -2.25; ZYME×37 yday $28.27 → 09:30 $29.32 +38.85; CLYM×68 yday $14.65 → 09:30 $13.60 -71.40; NVAX×121 yday $9.26 → 09:30 $9.37 +13.31; VIRT×16 yday $66.39 → 09:30 $65.64 -12.00; AMTX×592 yday $1.90 → 09:30 $1.87 -17.76; ESTC×13 yday $99.00 → 09:30 $96.54 -31.98; FIGR×29 yday $36.41 → 09:30 $36.80 +11.31 |
| 2026-09-02 | -3.83 | $3,362.13 | ZYME×37, CLYM×68, NVAX×121, VIRT×16, AMTX×592, ESTC×13, FIGR×29 | $10,936.27 | -11.13 | — | CLYM, VIRT, AMTX, ESTC, FIGR | $8,722.07 | $11,044.38 | ZYME×37, NVAX×121 | 09:30 open · cash $3,362.13 (unchanged overnight, no fees) · equity $10,936.27 vs prior close $10,947.40 (-11.13) because holdings re-marked: ZYME×37 yday $29.33 → 09:30 $29.32 -0.37; CLYM×68 yday $13.60 → 09:30 $13.88 +19.04; NVAX×121 yday $9.37 → 09:30 $9.20 -20.57; VIRT×16 yday $65.64 → 09:30 $65.38 -4.16; AMTX×592 yday $1.87 → 09:30 $1.88 +5.92; ESTC×13 yday $96.07 → 09:30 $95.76 -4.03; FIGR×29 yday $35.70 → 09:30 $35.46 -6.96 |
| 2026-09-03 | -0.90 | $8,722.07 | ZYME×37, NVAX×121 | $11,074.74 | +30.36 | OMER, SG, ATRC, RVTY, ARCT, TRLV, CLYM | NVAX | $74.92 | $11,325.70 | ZYME×37, OMER×75, SG×221, ATRC×28, RVTY×11, ARCT×86, TRLV×120, CLYM×96 | 09:30 open · cash $8,722.07 (unchanged overnight, no fees) · equity $11,074.74 vs prior close $11,044.38 (+30.36) because holdings re-marked: ZYME×37 yday $29.67 → 09:30 $30.00 +12.21; NVAX×121 yday $10.12 → 09:30 $10.27 +18.15 |
| 2026-09-04 | — | $74.92 | ZYME×37, OMER×75, SG×221, ATRC×28, RVTY×11, ARCT×86, TRLV×120, CLYM×96 | $11,297.27 | -28.43 | HQ, NVAX | — | $36.65 | $11,172.36 | ZYME×37, OMER×75, SG×221, ATRC×28, RVTY×11, ARCT×86, TRLV×120, CLYM×96, HQ×1, NVAX×2 | 09:30 open · cash $74.92 (unchanged overnight, no fees) · equity $11,297.27 vs prior close $11,325.70 (-28.43) because holdings re-marked: ZYME×37 yday $31.05 → 09:30 $31.34 +10.73; OMER×75 yday $18.86 → 09:30 $18.99 +9.75; SG×221 yday $6.73 → 09:30 $6.75 +4.42; ATRC×28 yday $52.59 → 09:30 $52.88 +8.12; RVTY×11 yday $130.94 → 09:30 $132.45 +16.61; ARCT×86 yday $16.74 → 09:30 $16.77 +2.58; TRLV×120 yday $11.69 → 09:30 $11.89 +24.00; CLYM×96 yday $15.05 → 09:30 $13.96 -104.64 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $8,761.36 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $7,544.34 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $6,293.15 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $5,049.62 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $3,782.66 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $2,547.94 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $1,349.89 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $107.38 | — | rank by candle_score; rank candle_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; SLS×106 yday $12.36 → 09:30 $12.40 +4.24 | — |
| 2026-08-14 09:30 ET | **BUY** | `SATL` | 2 | $5.98 | $0.13 | — | $95.30 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.9; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 1 | $9.89 | $0.10 | — | $85.30 | — | rank by candle_score; rank candle_score; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $13.42 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $85.30 | ▼ 09:30 equity $10,490.01 vs yday $10,517.00 (-26.99) | 09:30 open · cash $85.30 (unchanged overnight, no fees) · equity $10,490.01 vs prior close $10,517.00 (-26.99) because holdings re-marked: TNDM×53 yday $22.72 → 09:30 $22.50 -11.66; TPG×24 yday $53.03 → 09:30 $52.67 -8.64; HIMS×42 yday $28.15 → 09:30 $28.14 -0.42; IREN×27 yday $44.06 → 09:30 $45.23 +31.59; INO×1543 yday $1.09 → 09:30 $1.07 -30.86; VOR×56 yday $23.03 → 09:30 $22.91 -6.72; BTSG×20 yday $61.71 → 09:30 $61.69 -0.40; SLS×106 yday $12.78 → 09:30 $12.78 +0.00; SATL×2 yday $5.80 → 09:30 $5.81 +0.02; NMAX×1 yday $10.87 → 09:30 $10.97 +0.10 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 6 | $1.92 | $0.13 | — | $73.65 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $12.19 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 1 | $10.10 | $0.10 | — | $63.44 | — | rank by candle_score; rank candle_score; list mover_buy; ret5=+22.8; leftover $12.19 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 2 | $4.59 | $0.10 | — | $54.16 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.16 | ▼ 09:30 equity $10,446.92 vs yday $10,590.50 (-143.58) | 09:30 open · cash $54.16 (unchanged overnight, no fees) · equity $10,446.92 vs prior close $10,590.50 (-143.58) because holdings re-marked: TNDM×53 yday $22.25 → 09:30 $22.16 -5.03; TPG×24 yday $51.77 → 09:30 $51.77 +0.00; HIMS×42 yday $28.61 → 09:30 $27.85 -31.92; IREN×27 yday $44.90 → 09:30 $43.56 -36.18; INO×1543 yday $1.15 → 09:30 $1.14 -15.43; VOR×56 yday $23.01 → 09:30 $22.82 -10.64; BTSG×20 yday $60.38 → 09:30 $60.00 -7.60; SLS×106 yday $13.00 → 09:30 $12.66 -36.04; SATL×2 yday $5.81 → 09:30 $5.50 -0.62; NMAX×1 yday $10.36 → 09:30 $10.31 -0.05; NPWR×6 yday $1.73 → 09:30 $1.70 -0.18; SMJF×1 yday $10.45 → 09:30 $10.45 +0.00; BORR×2 yday $4.50 → 09:30 $4.56 +0.12 | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $1,226.48 | ▼ -66.33 after sell → book $10,444.76; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $2,466.87 | ▲ +23.38 after sell → book $10,442.67; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $3,634.44 | ▼ -83.63 after sell → book $10,440.54; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $4,808.47 | ▼ -69.50 after sell → book $10,438.45; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $6,547.31 | ▲ +471.89 after sell → book $10,418.27; vs 09:30 mark -20.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `VOR` | 56 | $22.82 | $2.18 | $+41.02 | $7,823.05 | ▲ +41.02 after sell → book $10,416.09; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $9,020.98 | ▼ -0.12 after sell → book $10,414.02; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $10,360.61 | ▲ +97.12 after sell → book $10,411.69; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,360.61 | ▲ 09:30 equity $10,413.68 vs yday $10,413.16 (+0.52) | 09:30 open · cash $10,360.61 (unchanged overnight, no fees) · equity $10,413.68 vs prior close $10,413.16 (+0.52) because holdings re-marked: SATL×2 yday $5.74 → 09:30 $5.82 +0.16; NMAX×1 yday $11.43 → 09:30 $11.50 +0.07; NPWR×6 yday $1.65 → 09:30 $1.70 +0.30; SMJF×1 yday $10.88 → 09:30 $10.71 -0.17; BORR×2 yday $4.43 → 09:30 $4.51 +0.16 | — |
| 2026-08-19 09:30 ET | **SELL** | `SATL` | 2 | $5.82 | $0.14 | $-0.59 | $10,372.10 | ▼ -0.59 after sell → book $10,413.53; vs 09:30 mark -0.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,372.10 | ▼ 09:30 equity $10,412.47 vs yday $10,412.55 (-0.08) | 09:30 open · cash $10,372.10 (unchanged overnight, no fees) · equity $10,412.47 vs prior close $10,412.55 (-0.08) because holdings re-marked: NMAX×1 yday $10.91 → 09:30 $10.89 -0.02; NPWR×6 yday $1.67 → 09:30 $1.64 -0.18; SMJF×1 yday $10.72 → 09:30 $10.72 +0.00; BORR×2 yday $4.40 → 09:30 $4.46 +0.12 | — |
| 2026-08-20 09:30 ET | **SELL** | `NMAX` | 1 | $10.89 | $0.13 | $+0.76 | $10,382.86 | ▲ +0.76 after sell → book $10,412.34; vs 09:30 mark -0.13 | dropped from list after 4 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 6 | $1.64 | $0.14 | $-1.95 | $10,392.57 | ▼ -1.95 after sell → book $10,412.21; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `SMJF` | 1 | $10.72 | $0.13 | $+0.39 | $10,403.16 | ▲ +0.39 after sell → book $10,412.08; vs 09:30 mark -0.13 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BORR` | 2 | $4.46 | $0.12 | $-0.47 | $10,411.96 | ▼ -0.47 after sell → book $10,411.96; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 19 | $65.60 | $2.05 | — | $9,163.51 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NBP` | 660 | $1.97 | $8.51 | — | $7,854.80 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ret5=+5.9; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 100 | $12.98 | $2.29 | — | $6,554.51 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 110 | $11.81 | $2.32 | — | $5,252.54 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $4,049.41 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1301.50 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 264 | $4.92 | $3.41 | — | $2,747.12 | — | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1131 | $1.15 | $14.59 | — | $1,431.88 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `GENB` | 77 | $16.76 | $2.22 | — | $139.14 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+12.5; leftover $1301.50 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $139.14 | ▲ 09:30 equity $10,478.53 vs yday $10,198.03 (+280.50) | 09:30 open · cash $139.14 (unchanged overnight, no fees) · equity $10,478.53 vs prior close $10,198.03 (+280.50) because holdings re-marked: IOND×19 yday $68.77 → 09:30 $68.41 -6.84; NBP×660 yday $1.91 → 09:30 $1.91 +0.00; IMMX×100 yday $13.16 → 09:30 $13.36 +20.00; ABCL×110 yday $11.57 → 09:30 $11.57 +0.00; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ABUS×264 yday $4.77 → 09:30 $5.20 +113.52; CYPH×1131 yday $1.19 → 09:30 $1.32 +147.03; GENB×77 yday $15.99 → 09:30 $16.10 +8.47 | — |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 2 | $9.08 | $0.19 | — | $120.79 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $19.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $109.55 | — | rank by candle_score; rank candle_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $19.88 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.55 | ▲ 09:30 equity $11,196.24 vs yday $10,760.39 (+435.85) | 09:30 open · cash $109.55 (unchanged overnight, no fees) · equity $11,196.24 vs prior close $10,760.39 (+435.85) because holdings re-marked: IOND×19 yday $68.73 → 09:30 $68.72 -0.19; NBP×660 yday $2.00 → 09:30 $2.01 +6.60; IMMX×100 yday $13.66 → 09:30 $13.69 +3.00; ABCL×110 yday $11.32 → 09:30 $10.97 -38.50; MRNA×8 yday $145.13 → 09:30 $142.70 -19.44; ABUS×264 yday $5.21 → 09:30 $5.18 -7.92; CYPH×1131 yday $1.42 → 09:30 $1.83 +463.71; GENB×77 yday $16.12 → 09:30 $16.50 +29.26; IOVA×2 yday $8.29 → 09:30 $8.05 -0.48; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.55 | ▲ 09:30 equity $11,016.73 vs yday $10,830.56 (+186.17) | 09:30 open · cash $109.55 (unchanged overnight, no fees) · equity $11,016.73 vs prior close $10,830.56 (+186.17) because holdings re-marked: IOND×19 yday $70.11 → 09:30 $68.27 -34.96; NBP×660 yday $1.87 → 09:30 $1.89 +13.20; IMMX×100 yday $13.35 → 09:30 $13.40 +5.00; ABCL×110 yday $10.52 → 09:30 $10.77 +27.50; MRNA×8 yday $139.27 → 09:30 $141.19 +15.36; ABUS×264 yday $5.20 → 09:30 $5.26 +15.84; CYPH×1131 yday $1.64 → 09:30 $1.70 +67.86; GENB×77 yday $16.76 → 09:30 $17.75 +76.23; IOVA×2 yday $8.22 → 09:30 $8.00 -0.44; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58 | — |
| 2026-08-25 09:30 ET | **SELL** | `IOND` | 19 | $68.27 | $2.07 | $+46.62 | $1,404.61 | ▲ +46.62 after sell → book $11,014.66; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NBP` | 660 | $1.89 | $8.63 | $-69.95 | $2,643.38 | ▼ -69.95 after sell → book $11,006.03; vs 09:30 mark -8.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IMMX` | 100 | $13.40 | $2.32 | $+37.39 | $3,981.06 | ▲ +37.39 after sell → book $11,003.71; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABCL` | 110 | $10.77 | $2.35 | $-119.62 | $5,163.41 | ▼ -119.62 after sell → book $11,001.36; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRNA` | 8 | $141.19 | $2.03 | $-75.65 | $6,290.90 | ▼ -75.65 after sell → book $10,999.33; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 264 | $5.26 | $3.46 | $+82.89 | $7,676.08 | ▲ +82.89 after sell → book $10,995.87; vs 09:30 mark -3.46 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `GENB` | 77 | $17.75 | $2.24 | $+71.76 | $9,040.58 | ▲ +71.76 after sell → book $10,993.62; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 68 | $18.75 | $2.19 | — | $7,763.39 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SG` | 184 | $7.00 | $2.54 | — | $6,472.85 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.3; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 94 | $13.70 | $2.27 | — | $5,182.77 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1291.51 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 137 | $9.36 | $2.40 | — | $3,898.05 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+21.3; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $2,701.43 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1291.51 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 117 | $11.02 | $2.34 | — | $1,409.75 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 52 | $24.73 | $2.15 | — | $121.65 | — | rank by candle_score; rank candle_score; list yday_gainer; ret5=+26.3; leftover $1291.51 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.65 | ▲ 09:30 equity $10,891.41 vs yday $10,891.41 (-0.00) | 09:30 open · cash $121.65 (unchanged overnight, no fees) · equity $10,891.41 vs prior close $10,891.41 (-0.00) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.64 +0.00; IOVA×2 yday $8.08 → 09:30 $8.08 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; OMER×68 yday $19.03 → 09:30 $19.03 +0.00; SG×184 yday $7.00 → 09:30 $7.00 +0.00; AVAH×94 yday $13.70 → 09:30 $13.70 +0.00; RUM×137 yday $9.35 → 09:30 $9.35 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; TRLV×117 yday $11.02 → 09:30 $11.02 +0.00; BMNR×52 yday $24.21 → 09:30 $24.21 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $121.65 | ▼ 09:30 equity $10,965.27 vs yday $10,977.71 (-12.44) | 09:30 open · cash $121.65 (unchanged overnight, no fees) · equity $10,965.27 vs prior close $10,977.71 (-12.44) because holdings re-marked: CYPH×1131 yday $1.64 → 09:30 $1.60 -45.24; IOVA×2 yday $8.08 → 09:30 $8.34 +0.52; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; OMER×68 yday $19.03 → 09:30 $18.96 -4.76; SG×184 yday $7.00 → 09:30 $6.95 -9.20; AVAH×94 yday $13.70 → 09:30 $13.65 -4.70; RUM×137 yday $9.35 → 09:30 $10.07 +98.64; AU×10 yday $118.55 → 09:30 $119.80 +12.50; TRLV×117 yday $11.02 → 09:30 $11.22 +23.40; BMNR×52 yday $24.21 → 09:30 $24.24 +1.56 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 1131 | $1.60 | $14.79 | $+479.57 | $1,916.45 | ▲ +479.57 after sell → book $10,950.47; vs 09:30 mark -14.80 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `IOVA` | 2 | $8.34 | $0.19 | $-1.86 | $1,932.94 | ▼ -1.86 after sell → book $10,950.28; vs 09:30 mark -0.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $1,948.11 | ▲ +3.93 after sell → book $10,950.10; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 5 | $40.72 | $2.00 | — | $1,742.51 | — | rank by candle_score; rank candle_score; list flatten; ret5=+1.8; leftover $243.51 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 8 | $28.89 | $2.01 | — | $1,509.38 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+1.6; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 15 | $15.60 | $2.04 | — | $1,273.34 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+7.1; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 9 | $24.84 | $2.02 | — | $1,047.76 | — | rank by candle_score; rank candle_score; list flatten; ret5=+13.0; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 1 | $170.60 | $1.71 | — | $875.45 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+3.4; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 94 | $2.59 | $2.27 | — | $629.72 | — | rank by candle_score; rank candle_score; list flatten; ret5=+4.2; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 11 | $21.97 | $2.02 | — | $386.03 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+0.6; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 3 | $75.12 | $2.00 | — | $158.67 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=-2.2; leftover $243.51 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $158.67 | ▲ 09:30 equity $10,889.89 vs yday $10,822.75 (+67.14) | 09:30 open · cash $158.67 (unchanged overnight, no fees) · equity $10,889.89 vs prior close $10,822.75 (+67.14) because holdings re-marked: OMER×68 yday $18.22 → 09:30 $18.24 +1.36; SG×184 yday $6.85 → 09:30 $6.87 +3.68; AVAH×94 yday $13.62 → 09:30 $13.62 +0.00; RUM×137 yday $9.38 → 09:30 $9.51 +17.81; AU×10 yday $118.11 → 09:30 $117.41 -7.00; TRLV×117 yday $11.43 → 09:30 $11.38 -5.85; BMNR×52 yday $24.91 → 09:30 $25.91 +52.00; RRC×5 yday $41.55 → 09:30 $41.44 -0.55; GEN×8 yday $29.64 → 09:30 $29.83 +1.52; DLO×15 yday $15.36 → 09:30 $15.33 -0.45; MOS×9 yday $24.16 → 09:30 $24.00 -1.44; PLTR×1 yday $177.50 → 09:30 $178.75 +1.25; SLI×94 yday $2.61 → 09:30 $2.60 -0.94; PGY×11 yday $22.41 → 09:30 $22.93 +5.72; MT×3 yday $74.53 → 09:30 $74.54 +0.03 | — |
| 2026-08-28 09:30 ET | **SELL** | `OMER` | 68 | $18.24 | $2.22 | $-39.09 | $1,396.78 | ▼ -39.09 after sell → book $10,887.68; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `SG` | 184 | $6.87 | $2.58 | $-29.04 | $2,658.27 | ▼ -29.04 after sell → book $10,885.09; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AVAH` | 94 | $13.62 | $2.30 | $-12.09 | $3,936.25 | ▼ -12.09 after sell → book $10,882.79; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RUM` | 137 | $9.51 | $2.43 | $+15.71 | $5,236.69 | ▲ +15.71 after sell → book $10,880.36; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 10 | $117.41 | $2.04 | $-24.56 | $6,408.75 | ▼ -24.56 after sell → book $10,878.32; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 52 | $25.91 | $2.17 | $+57.05 | $7,753.90 | ▲ +57.05 after sell → book $10,876.15; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 37 | $29.33 | $2.10 | — | $6,666.59 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1107.70 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CLYM` | 68 | $16.09 | $2.19 | — | $5,570.28 | — | rank by candle_score; rank candle_score; list yday_mover; ret5=+5.8; leftover $1107.70 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVAX` | 121 | $9.12 | $2.35 | — | $4,464.41 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.1; leftover $1107.70 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 16 | $65.42 | $2.04 | — | $3,415.65 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.2; leftover $1107.70 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 592 | $1.87 | $7.64 | — | $2,300.97 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+16.9; leftover $1107.70 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 13 | $82.64 | $2.03 | — | $1,224.62 | — | rank by candle_score; rank candle_score; list earn_react; ret5=-0.9; leftover $1107.70 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 29 | $37.42 | $2.08 | — | $137.36 | — | rank by candle_score; rank candle_score; list yday_mover; ret5=+24.4; leftover $1107.70 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.36 | ▲ 09:30 equity $11,039.17 vs yday $10,798.23 (+240.94) | 09:30 open · cash $137.36 (unchanged overnight, no fees) · equity $11,039.17 vs prior close $10,798.23 (+240.94) because holdings re-marked: TRLV×117 yday $11.03 → 09:30 $12.41 +161.46; RRC×5 yday $41.64 → 09:30 $41.11 -2.65; GEN×8 yday $30.50 → 09:30 $31.02 +4.16; DLO×15 yday $15.14 → 09:30 $15.01 -1.95; MOS×9 yday $23.76 → 09:30 $23.75 -0.09; PLTR×1 yday $185.93 → 09:30 $184.04 -1.89; SLI×94 yday $2.64 → 09:30 $2.51 -12.22; PGY×11 yday $23.26 → 09:30 $21.51 -19.25; MT×3 yday $74.63 → 09:30 $75.07 +1.32; ZYME×37 yday $29.01 → 09:30 $28.27 -27.38; CLYM×68 yday $15.06 → 09:30 $14.65 -27.88; NVAX×121 yday $9.05 → 09:30 $9.23 +21.78; VIRT×16 yday $67.04 → 09:30 $66.39 -10.40; AMTX×592 yday $1.87 → 09:30 $1.90 +17.76; ESTC×13 yday $83.74 → 09:30 $99.99 +211.25; FIGR×29 yday $38.02 → 09:30 $35.50 -73.08 | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 117 | $12.41 | $2.37 | $+157.92 | $1,586.96 | ▲ +157.92 after sell → book $11,036.80; vs 09:30 mark -2.37 | dropped from list after 4 sess (min 3) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,586.96 | ▼ 09:30 equity $11,001.43 vs yday $11,061.99 (-60.56) | 09:30 open · cash $1,586.96 (unchanged overnight, no fees) · equity $11,001.43 vs prior close $11,061.99 (-60.56) because holdings re-marked: RRC×5 yday $41.78 → 09:30 $41.32 -2.30; GEN×8 yday $31.02 → 09:30 $30.56 -3.68; DLO×15 yday $15.00 → 09:30 $14.88 -1.80; MOS×9 yday $23.78 → 09:30 $24.00 +1.98; PLTR×1 yday $183.80 → 09:30 $185.52 +1.72; SLI×94 yday $2.51 → 09:30 $2.70 +17.86; PGY×11 yday $21.95 → 09:30 $21.73 -2.42; MT×3 yday $75.06 → 09:30 $74.31 -2.25; ZYME×37 yday $28.27 → 09:30 $29.32 +38.85; CLYM×68 yday $14.65 → 09:30 $13.60 -71.40; NVAX×121 yday $9.26 → 09:30 $9.37 +13.31; VIRT×16 yday $66.39 → 09:30 $65.64 -12.00; AMTX×592 yday $1.90 → 09:30 $1.87 -17.76; ESTC×13 yday $99.00 → 09:30 $96.54 -31.98; FIGR×29 yday $36.41 → 09:30 $36.80 +11.31 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 5 | $41.32 | $2.02 | $-1.03 | $1,791.54 | ▼ -1.03 after sell → book $10,999.41; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 8 | $30.56 | $2.03 | $+9.31 | $2,033.98 | ▲ +9.31 after sell → book $10,997.37; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 15 | $14.88 | $2.06 | $-14.89 | $2,255.13 | ▼ -14.89 after sell → book $10,995.32; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MOS` | 9 | $24.00 | $2.04 | $-11.61 | $2,469.09 | ▼ -11.61 after sell → book $10,993.28; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PLTR` | 1 | $185.52 | $1.88 | $+11.33 | $2,652.73 | ▲ +11.33 after sell → book $10,991.40; vs 09:30 mark -1.88 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 94 | $2.70 | $2.30 | $+5.77 | $2,904.24 | ▲ +5.77 after sell → book $10,989.11; vs 09:30 mark -2.29 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `PGY` | 11 | $21.73 | $2.04 | $-6.71 | $3,141.22 | ▼ -6.71 after sell → book $10,987.06; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `MT` | 3 | $74.31 | $2.02 | $-6.45 | $3,362.13 | ▼ -6.45 after sell → book $10,985.04; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3,362.13 | ▼ 09:30 equity $10,936.27 vs yday $10,947.40 (-11.13) | 09:30 open · cash $3,362.13 (unchanged overnight, no fees) · equity $10,936.27 vs prior close $10,947.40 (-11.13) because holdings re-marked: ZYME×37 yday $29.33 → 09:30 $29.32 -0.37; CLYM×68 yday $13.60 → 09:30 $13.88 +19.04; NVAX×121 yday $9.37 → 09:30 $9.20 -20.57; VIRT×16 yday $65.64 → 09:30 $65.38 -4.16; AMTX×592 yday $1.87 → 09:30 $1.88 +5.92; ESTC×13 yday $96.07 → 09:30 $95.76 -4.03; FIGR×29 yday $35.70 → 09:30 $35.46 -6.96 | — |
| 2026-09-02 09:30 ET | **SELL** | `CLYM` | 68 | $13.88 | $2.22 | $-154.69 | $4,303.76 | ▼ -154.69 after sell → book $10,934.06; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VIRT` | 16 | $65.38 | $2.06 | $-4.74 | $5,347.78 | ▼ -4.74 after sell → book $10,932.00; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AMTX` | 592 | $1.88 | $7.75 | $-9.46 | $6,453.00 | ▼ -9.46 after sell → book $10,924.26; vs 09:30 mark -7.74 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ESTC` | 13 | $95.76 | $2.05 | $+166.48 | $7,695.83 | ▲ +166.48 after sell → book $10,922.21; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `FIGR` | 29 | $35.46 | $2.10 | $-61.01 | $8,722.07 | ▼ -61.01 after sell → book $10,920.11; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,722.07 | ▲ 09:30 equity $11,074.74 vs yday $11,044.38 (+30.36) | 09:30 open · cash $8,722.07 (unchanged overnight, no fees) · equity $11,074.74 vs prior close $11,044.38 (+30.36) because holdings re-marked: ZYME×37 yday $29.67 → 09:30 $30.00 +12.21; NVAX×121 yday $10.12 → 09:30 $10.27 +18.15 | — |
| 2026-09-03 09:30 ET | **SELL** | `NVAX` | 121 | $10.27 | $2.38 | $+134.41 | $9,962.36 | ▲ +134.41 after sell → book $11,072.36; vs 09:30 mark -2.38 | dropped from list after 4 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 75 | $18.97 | $2.21 | — | $8,537.39 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SG` | 221 | $6.43 | $2.85 | — | $7,113.51 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.3; leftover $1423.19 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 28 | $49.76 | $2.07 | — | $5,718.16 | — | rank by candle_score; rank candle_score; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 11 | $125.94 | $2.02 | — | $4,330.79 | — | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 86 | $16.46 | $2.25 | — | $2,912.99 | — | rank by candle_score; rank candle_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 120 | $11.78 | $2.35 | — | $1,497.04 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 96 | $14.79 | $2.28 | — | $74.92 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+5.8; leftover $1423.19 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.92 | ▼ 09:30 equity $11,297.27 vs yday $11,325.70 (-28.43) | 09:30 open · cash $74.92 (unchanged overnight, no fees) · equity $11,297.27 vs prior close $11,325.70 (-28.43) because holdings re-marked: ZYME×37 yday $31.05 → 09:30 $31.34 +10.73; OMER×75 yday $18.86 → 09:30 $18.99 +9.75; SG×221 yday $6.73 → 09:30 $6.75 +4.42; ATRC×28 yday $52.59 → 09:30 $52.88 +8.12; RVTY×11 yday $130.94 → 09:30 $132.45 +16.61; ARCT×86 yday $16.74 → 09:30 $16.77 +2.58; TRLV×120 yday $11.69 → 09:30 $11.89 +24.00; CLYM×96 yday $15.05 → 09:30 $13.96 -104.64 | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 1 | $17.06 | $0.17 | — | $57.68 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $24.97 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $36.65 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $24.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `ZS` | cash | leftover split 13.42 < 1 share @ 190.00 |
| 2026-08-14 | `BETA` | cash | leftover split 13.42 < 1 share @ 25.21 |
| 2026-08-14 | `BRZE` | cash | leftover split 13.42 < 1 share @ 30.00 |
| 2026-08-14 | `MH` | cash | leftover split 13.42 < 1 share @ 13.55 |
| 2026-08-14 | `GLOB` | cash | leftover split 13.42 < 1 share @ 38.21 |
| 2026-08-14 | `LUNR` | cash | leftover split 13.42 < 1 share @ 19.17 |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SATL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `JBIO` | cash | leftover split 12.19 < 1 share @ 24.60 |
| 2026-08-17 | `HTFL` | cash | leftover split 12.19 < 1 share @ 41.23 |
| 2026-08-17 | `STDN` | cash | leftover split 12.19 < 1 share @ 13.64 |
| 2026-08-17 | `CLYM` | cash | leftover split 12.19 < 1 share @ 16.25 |
| 2026-08-18 | `SATL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NMAX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `SMJF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BORR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADCT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CERS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYMR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `SMJF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BORR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `MTDR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSKY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RDZN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `IOND` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NBP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IMMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `GENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SM` | cash | leftover split 19.88 < 1 share @ 37.81 |
| 2026-08-21 | `ARIS` | cash | leftover split 19.88 < 1 share @ 20.90 |
| 2026-08-21 | `DXYZ` | cash | leftover split 19.88 < 1 share @ 34.89 |
| 2026-08-21 | `ILMN` | cash | leftover split 19.88 < 1 share @ 212.40 |
| 2026-08-21 | `AEM` | cash | leftover split 19.88 < 1 share @ 216.30 |
| 2026-08-24 | `IOND` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NBP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IMMX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `GENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IOVA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `IOVA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `IOVA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `ZYME` | no_price | no 09:30 open |
| 2026-08-26 | `NVAX` | no_price | no 09:30 open |
| 2026-08-27 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `TRLV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-28 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MOS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PLTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `PGY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PLTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `PGY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VIRT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `AMTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ESTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OMER` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CLYM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VIRT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `AMTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ESTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CELH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RANI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VIRT` | cash | leftover split 24.97 < 1 share @ 63.37 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ZYME` | 37 | 2026-08-28 @ $29.33 | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1107.70 |
| `OMER` | 75 | 2026-09-03 @ $18.97 | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1423.19 |
| `SG` | 221 | 2026-09-03 @ $6.43 | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.3; leftover $1423.19 |
| `ATRC` | 28 | 2026-09-03 @ $49.76 | rank by candle_score; rank candle_score; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1423.19 |
| `RVTY` | 11 | 2026-09-03 @ $125.94 | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1423.19 |
| `ARCT` | 86 | 2026-09-03 @ $16.46 | rank by candle_score; rank candle_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1423.19 |
| `TRLV` | 120 | 2026-09-03 @ $11.78 | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1423.19 |
| `CLYM` | 96 | 2026-09-03 @ $14.79 | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+5.8; leftover $1423.19 |
| `HQ` | 1 | 2026-09-04 @ $17.06 | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $24.97 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | rank by candle_score; rank candle_score; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $24.97 |
