# Factor mine action — `union_candle_score_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `candle_score` · size `leftover` · sell `list` · S-boost `none` · rank by candle_score

Cash book **+9.42%** ($10,942) · signal-only (no cash/fees) was +2.71%. Starts YES **16/17**. Fills 138 · skips 54 · realized $+898.18.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `candle_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $58.53.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TNDM, TPG, HIMS, IREN, INO, VOR, BTSG, SLS | — | $107.38 | $10,268.71 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $107.38 | TNDM×53, TPG×24, HIMS×42, IREN×27, INO×1543, VOR×56, BTSG×20, SLS×106 | $10,312.70 | +43.99 | ZS, BETA, SATL, BRZE, MH, NMAX, GLOB, LUNR | TNDM, TPG, HIMS, IREN, INO, VOR, BTSG, SLS | $224.74 | $10,166.44 | ZS×6, BETA×50, SATL×214, BRZE×42, MH×94, NMAX×129, GLOB×33, LUNR×67 | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; IREN×27 yday $44.76 → 09:30 $44.09 -18.09; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60; SLS×106 yday $12.36 → 09:30 $12.40 +4.24 |
| 2026-08-17 | +2.25 | $224.74 | ZS×6, BETA×50, SATL×214, BRZE×42, MH×94, NMAX×129, GLOB×33, LUNR×67 | $10,259.17 | +92.73 | NPWR, JBIO, HTFL, SMJF, STDN, CLYM, BORR | ZS, BETA, SATL, BRZE, MH, GLOB, LUNR | $38.01 | $10,059.68 | NMAX×129, NPWR×656, JBIO×51, HTFL×30, SMJF×124, STDN×92, CLYM×77, BORR×274 | 09:30 open · cash $224.74 (unchanged overnight, no fees) · equity $10,259.17 vs prior close $10,166.44 (+92.73) because holdings re-marked: ZS×6 yday $183.60 → 09:30 $188.38 +28.65; BETA×50 yday $24.86 → 09:30 $24.61 -12.50; SATL×214 yday $5.80 → 09:30 $5.81 +2.14; BRZE×42 yday $28.93 → 09:30 $28.44 -20.58; MH×94 yday $13.10 → 09:30 $13.16 +5.64; NMAX×129 yday $10.87 → 09:30 $10.97 +12.90; GLOB×33 yday $37.38 → 09:30 $37.18 -6.60; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08 |
| 2026-08-18 | -6.20 | $38.01 | NMAX×129, NPWR×656, JBIO×51, HTFL×30, SMJF×124, STDN×92, CLYM×77, BORR×274 | $9,975.83 | -83.85 | — | NMAX, NPWR, JBIO, HTFL, SMJF, STDN, CLYM, BORR | $9,950.06 | $9,950.06 | — | 09:30 open · cash $38.01 (unchanged overnight, no fees) · equity $9,975.83 vs prior close $10,059.68 (-83.85) because holdings re-marked: NMAX×129 yday $10.36 → 09:30 $10.31 -6.45; NPWR×656 yday $1.73 → 09:30 $1.70 -19.68; JBIO×51 yday $23.45 → 09:30 $23.07 -19.38; HTFL×30 yday $41.94 → 09:30 $41.50 -13.20; SMJF×124 yday $10.45 → 09:30 $10.45 +0.00; STDN×92 yday $13.31 → 09:30 $13.31 +0.00; CLYM×77 yday $17.44 → 09:30 $16.90 -41.58; BORR×274 yday $4.50 → 09:30 $4.56 +16.44 |
| 2026-08-19 | -7.20 | $9,950.06 | — | $9,950.06 | +0.00 | — | — | $9,950.06 | $9,950.06 | — | 09:30 open · cash $9,950.06 · no holdings · equity $9,950.06 vs prior close $9,950.06 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $9,950.06 | — | $9,950.06 | +0.00 | IOND, NBP, IMMX, ABCL, MRNA, ABUS, CYPH, GENB | — | $91.98 | $9,738.35 | IOND×18, NBP×631, IMMX×95, ABCL×105, MRNA×8, ABUS×252, CYPH×1081, GENB×74 | 09:30 open · cash $9,950.06 · no holdings · equity $9,950.06 vs prior close $9,950.06 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $91.98 | IOND×18, NBP×631, IMMX×95, ABCL×105, MRNA×8, ABUS×252, CYPH×1081, GENB×74 | $10,006.22 | +267.87 | SM, IOVA, ARIS, ARCT, DXYZ, ILMN, AEM | IOND, NBP, IMMX, ABCL, MRNA, ABUS, GENB | $325.06 | $10,219.61 | CYPH×1081, SM×32, IOVA×134, ARIS×58, ARCT×109, DXYZ×35, ILMN×5, AEM×5 | 09:30 open · cash $91.98 (unchanged overnight, no fees) · equity $10,006.22 vs prior close $9,738.35 (+267.87) because holdings re-marked: IOND×18 yday $68.77 → 09:30 $68.41 -6.48; NBP×631 yday $1.91 → 09:30 $1.91 +0.00; IMMX×95 yday $13.16 → 09:30 $13.36 +19.00; ABCL×105 yday $11.57 → 09:30 $11.57 +0.00; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ABUS×252 yday $4.77 → 09:30 $5.20 +108.36; CYPH×1081 yday $1.19 → 09:30 $1.32 +140.53; GENB×74 yday $15.99 → 09:30 $16.10 +8.14 |
| 2026-08-24 | -5.17 | $325.06 | CYPH×1081, SM×32, IOVA×134, ARIS×58, ARCT×109, DXYZ×35, ILMN×5, AEM×5 | $10,537.89 | +318.27 | — | CYPH, SM, IOVA, ARIS, ARCT, DXYZ, ILMN, AEM | $10,508.52 | $10,508.52 | — | 09:30 open · cash $325.06 (unchanged overnight, no fees) · equity $10,537.89 vs prior close $10,219.61 (+318.27) because holdings re-marked: CYPH×1081 yday $1.42 → 09:30 $1.83 +443.21; SM×32 yday $37.20 → 09:30 $36.51 -22.08; IOVA×134 yday $8.29 → 09:30 $8.05 -32.16; ARIS×58 yday $20.86 → 09:30 $20.98 +6.96; ARCT×109 yday $13.45 → 09:30 $13.26 -20.71; DXYZ×35 yday $34.43 → 09:30 $33.12 -45.85; ILMN×5 yday $219.40 → 09:30 $216.21 -15.95; AEM×5 yday $216.06 → 09:30 $217.03 +4.85 |
| 2026-08-25 | +1.80 | $10,508.52 | — | $10,508.52 | -0.00 | OMER, SG, AVAH, CYPH, RUM, AU, TRLV, BMNR | — | $120.14 | $10,417.83 | OMER×70, SG×187, AVAH×95, CYPH×772, RUM×140, AU×10, TRLV×119, BMNR×53 | 09:30 open · cash $10,508.52 · no holdings · equity $10,508.52 vs prior close $10,508.52 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $120.14 | OMER×70, SG×187, AVAH×95, CYPH×772, RUM×140, AU×10, TRLV×119, BMNR×53 | $10,417.83 | -0.00 | — | — | $120.14 | $10,482.61 | OMER×70, SG×187, AVAH×95, CYPH×772, RUM×140, AU×10, TRLV×119, BMNR×53 | 09:30 open · cash $120.14 (unchanged overnight, no fees) · equity $10,417.83 vs prior close $10,417.83 (-0.00) because holdings re-marked: OMER×70 yday $19.03 → 09:30 $19.03 +0.00; SG×187 yday $7.00 → 09:30 $7.00 +0.00; AVAH×95 yday $13.70 → 09:30 $13.70 +0.00; CYPH×772 yday $1.64 → 09:30 $1.64 +0.00; RUM×140 yday $9.35 → 09:30 $9.35 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; TRLV×119 yday $11.02 → 09:30 $11.02 +0.00; BMNR×53 yday $24.21 → 09:30 $24.21 +0.00 |
| 2026-08-27 | — | $120.14 | OMER×70, SG×187, AVAH×95, CYPH×772, RUM×140, AU×10, TRLV×119, BMNR×53 | $10,506.64 | +24.03 | RRC, GEN, DLO, MOS, PLTR, SLI, PGY, MT | OMER, SG, AVAH, CYPH, RUM, AU, TRLV, BMNR | $194.07 | $10,538.42 | RRC×32, GEN×45, DLO×83, MOS×52, PLTR×7, SLI×505, PGY×59, MT×17 | 09:30 open · cash $120.14 (unchanged overnight, no fees) · equity $10,506.64 vs prior close $10,482.61 (+24.03) because holdings re-marked: OMER×70 yday $19.03 → 09:30 $18.96 -4.90; SG×187 yday $7.00 → 09:30 $6.95 -9.35; AVAH×95 yday $13.70 → 09:30 $13.65 -4.75; CYPH×772 yday $1.64 → 09:30 $1.60 -30.88; RUM×140 yday $9.35 → 09:30 $10.07 +100.80; AU×10 yday $118.55 → 09:30 $119.80 +12.50; TRLV×119 yday $11.02 → 09:30 $11.22 +23.80; BMNR×53 yday $24.21 → 09:30 $24.24 +1.59 |
| 2026-08-28 | +0.75 | $194.07 | RRC×32, GEN×45, DLO×83, MOS×52, PLTR×7, SLI×505, PGY×59, MT×17 | $10,567.19 | +28.77 | TRLV, ZYME, CLYM, NVAX, VIRT, AMTX, ESTC, FIGR | RRC, GEN, DLO, MOS, PLTR, SLI, PGY, MT | $131.28 | $10,443.31 | TRLV×115, ZYME×44, CLYM×81, NVAX×144, VIRT×20, AMTX×704, ESTC×15, FIGR×35 | 09:30 open · cash $194.07 (unchanged overnight, no fees) · equity $10,567.19 vs prior close $10,538.42 (+28.77) because holdings re-marked: RRC×32 yday $41.55 → 09:30 $41.44 -3.52; GEN×45 yday $29.64 → 09:30 $29.83 +8.55; DLO×83 yday $15.36 → 09:30 $15.33 -2.49; MOS×52 yday $24.16 → 09:30 $24.00 -8.32; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; SLI×505 yday $2.61 → 09:30 $2.60 -5.05; PGY×59 yday $22.41 → 09:30 $22.93 +30.68; MT×17 yday $74.53 → 09:30 $74.54 +0.17 |
| 2026-08-31 | -5.85 | $131.28 | TRLV×115, ZYME×44, CLYM×81, NVAX×144, VIRT×20, AMTX×704, ESTC×15, FIGR×35 | $10,725.83 | +282.52 | — | TRLV, CLYM, VIRT, AMTX, ESTC, FIGR | $8,132.76 | $10,710.08 | ZYME×44, NVAX×144 | 09:30 open · cash $131.28 (unchanged overnight, no fees) · equity $10,725.83 vs prior close $10,443.31 (+282.52) because holdings re-marked: TRLV×115 yday $11.03 → 09:30 $12.41 +158.70; ZYME×44 yday $29.01 → 09:30 $28.27 -32.56; CLYM×81 yday $15.06 → 09:30 $14.65 -33.21; NVAX×144 yday $9.05 → 09:30 $9.23 +25.92; VIRT×20 yday $67.04 → 09:30 $66.39 -13.00; AMTX×704 yday $1.87 → 09:30 $1.90 +21.12; ESTC×15 yday $83.74 → 09:30 $99.99 +243.75; FIGR×35 yday $38.02 → 09:30 $35.50 -88.20 |
| 2026-09-01 | -6.30 | $8,132.76 | ZYME×44, NVAX×144 | $10,772.12 | +62.04 | — | ZYME | $9,420.70 | $10,769.98 | NVAX×144 | 09:30 open · cash $8,132.76 (unchanged overnight, no fees) · equity $10,772.12 vs prior close $10,710.08 (+62.04) because holdings re-marked: ZYME×44 yday $28.27 → 09:30 $29.32 +46.20; NVAX×144 yday $9.26 → 09:30 $9.37 +15.84 |
| 2026-09-02 | -3.83 | $9,420.70 | NVAX×144 | $10,745.50 | -24.48 | — | — | $9,420.70 | $10,877.98 | NVAX×144 | 09:30 open · cash $9,420.70 (unchanged overnight, no fees) · equity $10,745.50 vs prior close $10,769.98 (-24.48) because holdings re-marked: NVAX×144 yday $9.37 → 09:30 $9.20 -24.48 |
| 2026-09-03 | -0.90 | $9,420.70 | NVAX×144 | $10,899.58 | +21.60 | OMER, SG, ATRC, RVTY, ARCT, TRLV, ZYME, CLYM | NVAX | $157.52 | $11,144.82 | OMER×71, SG×211, ATRC×27, RVTY×10, ARCT×82, TRLV×115, ZYME×45, CLYM×92 | 09:30 open · cash $9,420.70 (unchanged overnight, no fees) · equity $10,899.58 vs prior close $10,877.98 (+21.60) because holdings re-marked: NVAX×144 yday $10.12 → 09:30 $10.27 +21.60 |
| 2026-09-04 | — | $157.52 | OMER×71, SG×211, ATRC×27, RVTY×10, ARCT×82, TRLV×115, ZYME×45, CLYM×92 | $11,119.43 | -25.39 | HQ, NVAX, VIRT | RVTY, ARCT, CLYM | $58.53 | $10,941.66 | OMER×71, SG×211, ATRC×27, TRLV×115, ZYME×45, HQ×80, NVAX×132, VIRT×21 | 09:30 open · cash $157.52 (unchanged overnight, no fees) · equity $11,119.43 vs prior close $11,144.82 (-25.39) because holdings re-marked: OMER×71 yday $18.86 → 09:30 $18.99 +9.23; SG×211 yday $6.73 → 09:30 $6.75 +4.22; ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; ARCT×82 yday $16.74 → 09:30 $16.77 +2.46; TRLV×115 yday $11.69 → 09:30 $11.89 +23.00; ZYME×45 yday $31.05 → 09:30 $31.34 +13.05; CLYM×92 yday $15.05 → 09:30 $13.96 -100.28 |

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
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $1,319.97 | ▼ -26.05 after sell → book $10,310.53; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $2,644.85 | ▲ +107.86 after sell → book $10,308.45; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $3,867.01 | ▼ -29.03 after sell → book $10,306.31; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $5,055.35 | ▼ -55.19 after sell → book $10,304.22; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $6,471.10 | ▲ +148.79 after sell → book $10,284.98; vs 09:30 mark -19.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $7,775.40 | ▲ +69.58 after sell → book $10,282.80; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $8,966.33 | ▼ -7.12 after sell → book $10,280.73; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $10,278.39 | ▲ +69.56 after sell → book $10,278.39; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `ZS` | 6 | $190.00 | $2.01 | — | $9,136.38 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETA` | 50 | $25.21 | $2.14 | — | $7,873.74 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+15.5; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SATL` | 214 | $5.98 | $2.76 | — | $6,591.26 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.9; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BRZE` | 42 | $30.00 | $2.12 | — | $5,329.15 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.2; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MH` | 94 | $13.55 | $2.27 | — | $4,053.18 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+17.5; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NMAX` | 129 | $9.89 | $2.38 | — | $2,774.34 | — | rank by candle_score; rank candle_score; list ohlc_hot,earn_react; 🔵; ⚪; ret5=+10.9; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `GLOB` | 33 | $38.21 | $2.09 | — | $1,511.32 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+10.0; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $224.74 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $224.74 | ▲ 09:30 equity $10,259.17 vs yday $10,166.44 (+92.73) | 09:30 open · cash $224.74 (unchanged overnight, no fees) · equity $10,259.17 vs prior close $10,166.44 (+92.73) because holdings re-marked: ZS×6 yday $183.60 → 09:30 $188.38 +28.65; BETA×50 yday $24.86 → 09:30 $24.61 -12.50; SATL×214 yday $5.80 → 09:30 $5.81 +2.14; BRZE×42 yday $28.93 → 09:30 $28.44 -20.58; MH×94 yday $13.10 → 09:30 $13.16 +5.64; NMAX×129 yday $10.87 → 09:30 $10.97 +12.90; GLOB×33 yday $37.38 → 09:30 $37.18 -6.60; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `ZS` | 6 | $188.38 | $2.03 | $-13.79 | $1,352.97 | ▼ -13.79 after sell → book $10,257.15; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETA` | 50 | $24.61 | $2.16 | $-34.30 | $2,581.31 | ▼ -34.30 after sell → book $10,254.99; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SATL` | 214 | $5.81 | $2.81 | $-41.95 | $3,821.84 | ▼ -41.95 after sell → book $10,252.18; vs 09:30 mark -2.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BRZE` | 42 | $28.44 | $2.14 | $-69.77 | $5,014.18 | ▼ -69.77 after sell → book $10,250.04; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MH` | 94 | $13.16 | $2.30 | $-41.23 | $6,248.93 | ▼ -41.23 after sell → book $10,247.75; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `GLOB` | 33 | $37.18 | $2.11 | $-38.19 | $7,473.76 | ▼ -38.19 after sell → book $10,245.64; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $8,828.29 | ▲ +67.96 after sell → book $10,243.42; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 656 | $1.92 | $8.46 | — | $7,560.31 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1261.18 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `JBIO` | 51 | $24.60 | $2.14 | — | $6,303.57 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.5; leftover $1261.18 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 30 | $41.23 | $2.08 | — | $5,064.59 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $1261.18 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 124 | $10.10 | $2.36 | — | $3,809.83 | — | rank by candle_score; rank candle_score; list mover_buy; ret5=+22.8; leftover $1261.18 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 92 | $13.64 | $2.27 | — | $2,552.68 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1261.18 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CLYM` | 77 | $16.25 | $2.22 | — | $1,299.21 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+16.6; leftover $1261.18 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BORR` | 274 | $4.59 | $3.53 | — | $38.01 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ⚪; ret5=+14.8; leftover $1261.18 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.01 | ▼ 09:30 equity $9,975.83 vs yday $10,059.68 (-83.85) | 09:30 open · cash $38.01 (unchanged overnight, no fees) · equity $9,975.83 vs prior close $10,059.68 (-83.85) because holdings re-marked: NMAX×129 yday $10.36 → 09:30 $10.31 -6.45; NPWR×656 yday $1.73 → 09:30 $1.70 -19.68; JBIO×51 yday $23.45 → 09:30 $23.07 -19.38; HTFL×30 yday $41.94 → 09:30 $41.50 -13.20; SMJF×124 yday $10.45 → 09:30 $10.45 +0.00; STDN×92 yday $13.31 → 09:30 $13.31 +0.00; CLYM×77 yday $17.44 → 09:30 $16.90 -41.58; BORR×274 yday $4.50 → 09:30 $4.56 +16.44 | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 129 | $10.31 | $2.41 | $+48.75 | $1,365.60 | ▲ +48.75 after sell → book $9,973.43; vs 09:30 mark -2.40 | dropped from list after 2 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 656 | $1.70 | $8.58 | $-161.36 | $2,472.21 | ▼ -161.36 after sell → book $9,964.84; vs 09:30 mark -8.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `JBIO` | 51 | $23.07 | $2.16 | $-82.34 | $3,646.62 | ▼ -82.34 after sell → book $9,962.68; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 30 | $41.50 | $2.10 | $+3.92 | $4,889.52 | ▲ +3.92 after sell → book $9,960.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 124 | $10.45 | $2.39 | $+38.64 | $6,182.93 | ▲ +38.64 after sell → book $9,958.19; vs 09:30 mark -2.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 92 | $13.31 | $2.29 | $-34.92 | $7,405.16 | ▼ -34.92 after sell → book $9,955.90; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `CLYM` | 77 | $16.90 | $2.24 | $+45.58 | $8,704.21 | ▲ +45.58 after sell → book $9,953.65; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `BORR` | 274 | $4.56 | $3.59 | $-15.34 | $9,950.06 | ▼ -15.34 after sell → book $9,950.06; vs 09:30 mark -3.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.06 | ▲ 09:30 equity $9,950.06 vs yday $9,950.06 (+0.00) | 09:30 open · cash $9,950.06 · no holdings · equity $9,950.06 vs prior close $9,950.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,950.06 | ▲ 09:30 equity $9,950.06 vs yday $9,950.06 (+0.00) | 09:30 open · cash $9,950.06 · no holdings · equity $9,950.06 vs prior close $9,950.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `IOND` | 18 | $65.60 | $2.04 | — | $8,767.22 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ⚪; ret5=+3.7; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NBP` | 631 | $1.97 | $8.14 | — | $7,516.01 | — | rank by candle_score; rank candle_score; list earn_react; 🔵; ret5=+5.9; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IMMX` | 95 | $12.98 | $2.27 | — | $6,280.63 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 105 | $11.81 | $2.31 | — | $5,037.75 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 8 | $150.14 | $2.01 | — | $3,834.62 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1243.76 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 252 | $4.92 | $3.25 | — | $2,591.53 | — | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 1081 | $1.15 | $13.94 | — | $1,334.43 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `GENB` | 74 | $16.76 | $2.21 | — | $91.98 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+12.5; leftover $1243.76 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $91.98 | ▲ 09:30 equity $10,006.22 vs yday $9,738.35 (+267.87) | 09:30 open · cash $91.98 (unchanged overnight, no fees) · equity $10,006.22 vs prior close $9,738.35 (+267.87) because holdings re-marked: IOND×18 yday $68.77 → 09:30 $68.41 -6.48; NBP×631 yday $1.91 → 09:30 $1.91 +0.00; IMMX×95 yday $13.16 → 09:30 $13.36 +19.00; ABCL×105 yday $11.57 → 09:30 $11.57 +0.00; MRNA×8 yday $133.32 → 09:30 $133.11 -1.68; ABUS×252 yday $4.77 → 09:30 $5.20 +108.36; CYPH×1081 yday $1.19 → 09:30 $1.32 +140.53; GENB×74 yday $15.99 → 09:30 $16.10 +8.14 | — |
| 2026-08-21 09:30 ET | **SELL** | `IOND` | 18 | $68.41 | $2.06 | $+46.47 | $1,321.30 | ▲ +46.47 after sell → book $10,004.16; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `NBP` | 631 | $1.91 | $8.25 | $-54.25 | $2,518.25 | ▼ -54.25 after sell → book $9,995.90; vs 09:30 mark -8.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IMMX` | 95 | $13.36 | $2.30 | $+31.52 | $3,785.15 | ▲ +31.52 after sell → book $9,993.60; vs 09:30 mark -2.30 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 105 | $11.57 | $2.33 | $-30.36 | $4,997.67 | ▼ -30.36 after sell → book $9,991.27; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MRNA` | 8 | $133.11 | $2.03 | $-140.29 | $6,060.52 | ▼ -140.29 after sell → book $9,989.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 252 | $5.20 | $3.30 | $+64.01 | $7,367.61 | ▲ +64.01 after sell → book $9,985.93; vs 09:30 mark -3.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `GENB` | 74 | $16.10 | $2.23 | $-53.29 | $8,556.78 | ▼ -53.29 after sell → book $9,983.70; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `SM` | 32 | $37.81 | $2.09 | — | $7,344.77 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $1222.40 | join🟢 sector🔴 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 134 | $9.08 | $2.39 | — | $6,125.66 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARIS` | 58 | $20.90 | $2.16 | — | $4,911.30 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $3,695.81 | — | rank by candle_score; rank candle_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DXYZ` | 35 | $34.89 | $2.10 | — | $2,472.57 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+8.6; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ILMN` | 5 | $212.40 | $2.00 | — | $1,408.56 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+10.7; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $325.06 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1222.40 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $325.06 | ▲ 09:30 equity $10,537.89 vs yday $10,219.61 (+318.27) | 09:30 open · cash $325.06 (unchanged overnight, no fees) · equity $10,537.89 vs prior close $10,219.61 (+318.27) because holdings re-marked: CYPH×1081 yday $1.42 → 09:30 $1.83 +443.21; SM×32 yday $37.20 → 09:30 $36.51 -22.08; IOVA×134 yday $8.29 → 09:30 $8.05 -32.16; ARIS×58 yday $20.86 → 09:30 $20.98 +6.96; ARCT×109 yday $13.45 → 09:30 $13.26 -20.71; DXYZ×35 yday $34.43 → 09:30 $33.12 -45.85; ILMN×5 yday $219.40 → 09:30 $216.21 -15.95; AEM×5 yday $216.06 → 09:30 $217.03 +4.85 | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1081 | $1.83 | $14.14 | $+706.99 | $2,289.14 | ▲ +706.99 after sell → book $10,523.74; vs 09:30 mark -14.15 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `SM` | 32 | $36.51 | $2.11 | $-45.79 | $3,455.36 | ▼ -45.79 after sell → book $10,521.64; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 134 | $8.05 | $2.42 | $-142.84 | $4,531.63 | ▼ -142.84 after sell → book $10,519.21; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARIS` | 58 | $20.98 | $2.18 | $+0.29 | $5,746.29 | ▲ +0.29 after sell → book $10,517.03; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.26 | $2.35 | $+227.51 | $7,189.28 | ▲ +227.51 after sell → book $10,514.68; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `DXYZ` | 35 | $33.12 | $2.12 | $-66.16 | $8,346.37 | ▼ -66.16 after sell → book $10,512.57; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ILMN` | 5 | $216.21 | $2.02 | $+15.02 | $9,425.39 | ▲ +15.02 after sell → book $10,510.54; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $10,508.52 | ▼ -0.38 after sell → book $10,508.52; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,508.52 | ▲ 09:30 equity $10,508.52 vs yday $10,508.52 (-0.00) | 09:30 open · cash $10,508.52 · no holdings · equity $10,508.52 vs prior close $10,508.52 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `OMER` | 70 | $18.75 | $2.20 | — | $9,193.82 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1313.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SG` | 187 | $7.00 | $2.55 | — | $7,882.27 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+11.3; leftover $1313.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AVAH` | 95 | $13.70 | $2.27 | — | $6,578.49 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1313.56 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 772 | $1.70 | $9.96 | — | $5,256.13 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1313.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 140 | $9.36 | $2.41 | — | $3,943.32 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+21.3; leftover $1313.56 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $2,746.70 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1313.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `TRLV` | 119 | $11.02 | $2.35 | — | $1,432.98 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1313.56 | join🔴 sector🟡 gen🟡 news🟢 digest🟢 ab🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 53 | $24.73 | $2.15 | — | $120.14 | — | rank by candle_score; rank candle_score; list yday_gainer; ret5=+26.3; leftover $1313.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.14 | ▲ 09:30 equity $10,417.83 vs yday $10,417.83 (-0.00) | 09:30 open · cash $120.14 (unchanged overnight, no fees) · equity $10,417.83 vs prior close $10,417.83 (-0.00) because holdings re-marked: OMER×70 yday $19.03 → 09:30 $19.03 +0.00; SG×187 yday $7.00 → 09:30 $7.00 +0.00; AVAH×95 yday $13.70 → 09:30 $13.70 +0.00; CYPH×772 yday $1.64 → 09:30 $1.64 +0.00; RUM×140 yday $9.35 → 09:30 $9.35 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; TRLV×119 yday $11.02 → 09:30 $11.02 +0.00; BMNR×53 yday $24.21 → 09:30 $24.21 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.14 | ▲ 09:30 equity $10,506.64 vs yday $10,482.61 (+24.03) | 09:30 open · cash $120.14 (unchanged overnight, no fees) · equity $10,506.64 vs prior close $10,482.61 (+24.03) because holdings re-marked: OMER×70 yday $19.03 → 09:30 $18.96 -4.90; SG×187 yday $7.00 → 09:30 $6.95 -9.35; AVAH×95 yday $13.70 → 09:30 $13.65 -4.75; CYPH×772 yday $1.64 → 09:30 $1.60 -30.88; RUM×140 yday $9.35 → 09:30 $10.07 +100.80; AU×10 yday $118.55 → 09:30 $119.80 +12.50; TRLV×119 yday $11.02 → 09:30 $11.22 +23.80; BMNR×53 yday $24.21 → 09:30 $24.24 +1.59 | — |
| 2026-08-27 09:30 ET | **SELL** | `OMER` | 70 | $18.96 | $2.22 | $+10.28 | $1,445.12 | ▲ +10.28 after sell → book $10,504.42; vs 09:30 mark -2.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SG` | 187 | $6.95 | $2.59 | $-14.49 | $2,742.17 | ▼ -14.49 after sell → book $10,501.82; vs 09:30 mark -2.60 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AVAH` | 95 | $13.65 | $2.30 | $-9.33 | $4,036.62 | ▼ -9.33 after sell → book $10,499.52; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 772 | $1.60 | $10.10 | $-97.26 | $5,261.73 | ▼ -97.26 after sell → book $10,489.43; vs 09:30 mark -10.09 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 140 | $10.07 | $2.44 | $+94.55 | $6,669.08 | ▲ +94.55 after sell → book $10,486.98; vs 09:30 mark -2.45 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $7,865.04 | ▼ -0.66 after sell → book $10,484.94; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `TRLV` | 119 | $11.22 | $2.38 | $+19.08 | $9,197.84 | ▲ +19.08 after sell → book $10,482.56; vs 09:30 mark -2.38 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 53 | $24.24 | $2.17 | $-30.29 | $10,480.39 | ▼ -30.29 after sell → book $10,480.39; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 32 | $40.72 | $2.09 | — | $9,175.27 | — | rank by candle_score; rank candle_score; list flatten; ret5=+1.8; leftover $1310.05 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 45 | $28.89 | $2.12 | — | $7,873.09 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+1.6; leftover $1310.05 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 83 | $15.60 | $2.24 | — | $6,576.05 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+7.1; leftover $1310.05 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 52 | $24.84 | $2.15 | — | $5,282.23 | — | rank by candle_score; rank candle_score; list flatten; ret5=+13.0; leftover $1310.05 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $4,086.02 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+3.4; leftover $1310.05 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 505 | $2.59 | $6.51 | — | $2,771.55 | — | rank by candle_score; rank candle_score; list flatten; ret5=+4.2; leftover $1310.05 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PGY` | 59 | $21.97 | $2.17 | — | $1,473.16 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=+0.6; leftover $1310.05 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $75.12 | $2.04 | — | $194.07 | — | rank by candle_score; rank candle_score; list mover_buy; 🔵; ret5=-2.2; leftover $1310.05 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $194.07 | ▲ 09:30 equity $10,567.19 vs yday $10,538.42 (+28.77) | 09:30 open · cash $194.07 (unchanged overnight, no fees) · equity $10,567.19 vs prior close $10,538.42 (+28.77) because holdings re-marked: RRC×32 yday $41.55 → 09:30 $41.44 -3.52; GEN×45 yday $29.64 → 09:30 $29.83 +8.55; DLO×83 yday $15.36 → 09:30 $15.33 -2.49; MOS×52 yday $24.16 → 09:30 $24.00 -8.32; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; SLI×505 yday $2.61 → 09:30 $2.60 -5.05; PGY×59 yday $22.41 → 09:30 $22.93 +30.68; MT×17 yday $74.53 → 09:30 $74.54 +0.17 | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 32 | $41.44 | $2.11 | $+18.85 | $1,518.05 | ▲ +18.85 after sell → book $10,565.09; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 45 | $29.83 | $2.15 | $+38.03 | $2,858.25 | ▲ +38.03 after sell → book $10,562.94; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 83 | $15.33 | $2.26 | $-26.91 | $4,128.38 | ▼ -26.91 after sell → book $10,560.68; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 52 | $24.00 | $2.17 | $-47.99 | $5,374.21 | ▼ -47.99 after sell → book $10,558.51; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $6,623.43 | ▲ +53.01 after sell → book $10,556.48; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 505 | $2.60 | $6.61 | $-8.07 | $7,929.82 | ▼ -8.07 after sell → book $10,549.87; vs 09:30 mark -6.61 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PGY` | 59 | $22.93 | $2.19 | $+52.29 | $9,280.51 | ▲ +52.29 after sell → book $10,547.69; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $74.54 | $2.06 | $-13.96 | $10,545.62 | ▼ -13.96 after sell → book $10,545.62; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 115 | $11.38 | $2.33 | — | $9,234.59 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+15.0; leftover $1318.20 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 44 | $29.33 | $2.12 | — | $7,941.95 | — | rank by candle_score; rank candle_score; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1318.20 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CLYM` | 81 | $16.09 | $2.23 | — | $6,636.42 | — | rank by candle_score; rank candle_score; list yday_mover; ret5=+5.8; leftover $1318.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVAX` | 144 | $9.12 | $2.42 | — | $5,320.72 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.1; leftover $1318.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 20 | $65.42 | $2.05 | — | $4,010.27 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+13.2; leftover $1318.20 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AMTX` | 704 | $1.87 | $9.08 | — | $2,684.71 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+16.9; leftover $1318.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ESTC` | 15 | $82.64 | $2.04 | — | $1,443.08 | — | rank by candle_score; rank candle_score; list earn_react; ret5=-0.9; leftover $1318.20 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 35 | $37.42 | $2.10 | — | $131.28 | — | rank by candle_score; rank candle_score; list yday_mover; ret5=+24.4; leftover $1318.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $131.28 | ▲ 09:30 equity $10,725.83 vs yday $10,443.31 (+282.52) | 09:30 open · cash $131.28 (unchanged overnight, no fees) · equity $10,725.83 vs prior close $10,443.31 (+282.52) because holdings re-marked: TRLV×115 yday $11.03 → 09:30 $12.41 +158.70; ZYME×44 yday $29.01 → 09:30 $28.27 -32.56; CLYM×81 yday $15.06 → 09:30 $14.65 -33.21; NVAX×144 yday $9.05 → 09:30 $9.23 +25.92; VIRT×20 yday $67.04 → 09:30 $66.39 -13.00; AMTX×704 yday $1.87 → 09:30 $1.90 +21.12; ESTC×15 yday $83.74 → 09:30 $99.99 +243.75; FIGR×35 yday $38.02 → 09:30 $35.50 -88.20 | — |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 115 | $12.41 | $2.37 | $+113.75 | $1,556.07 | ▲ +113.75 after sell → book $10,723.47; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CLYM` | 81 | $14.65 | $2.26 | $-121.13 | $2,740.46 | ▼ -121.13 after sell → book $10,721.21; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VIRT` | 20 | $66.39 | $2.07 | $+15.28 | $4,066.19 | ▲ +15.28 after sell → book $10,719.14; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `AMTX` | 704 | $1.90 | $9.21 | $+2.83 | $5,394.58 | ▲ +2.83 after sell → book $10,709.93; vs 09:30 mark -9.21 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ESTC` | 15 | $99.99 | $2.06 | $+256.16 | $6,892.37 | ▲ +256.16 after sell → book $10,707.87; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 35 | $35.50 | $2.12 | $-71.41 | $8,132.76 | ▼ -71.41 after sell → book $10,705.76; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,132.76 | ▲ 09:30 equity $10,772.12 vs yday $10,710.08 (+62.04) | 09:30 open · cash $8,132.76 (unchanged overnight, no fees) · equity $10,772.12 vs prior close $10,710.08 (+62.04) because holdings re-marked: ZYME×44 yday $28.27 → 09:30 $29.32 +46.20; NVAX×144 yday $9.26 → 09:30 $9.37 +15.84 | — |
| 2026-09-01 09:30 ET | **SELL** | `ZYME` | 44 | $29.32 | $2.14 | $-4.70 | $9,420.70 | ▼ -4.70 after sell → book $10,769.98; vs 09:30 mark -2.14 | dropped from list after 2 sess (min 1) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,420.70 | ▼ 09:30 equity $10,745.50 vs yday $10,769.98 (-24.48) | 09:30 open · cash $9,420.70 (unchanged overnight, no fees) · equity $10,745.50 vs prior close $10,769.98 (-24.48) because holdings re-marked: NVAX×144 yday $9.37 → 09:30 $9.20 -24.48 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,420.70 | ▲ 09:30 equity $10,899.58 vs yday $10,877.98 (+21.60) | 09:30 open · cash $9,420.70 (unchanged overnight, no fees) · equity $10,899.58 vs prior close $10,877.98 (+21.60) because holdings re-marked: NVAX×144 yday $10.12 → 09:30 $10.27 +21.60 | — |
| 2026-09-03 09:30 ET | **SELL** | `NVAX` | 144 | $10.27 | $2.46 | $+160.72 | $10,897.12 | ▲ +160.72 after sell → book $10,897.12; vs 09:30 mark -2.46 | dropped from list after 4 sess (min 1) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 71 | $18.97 | $2.20 | — | $9,548.04 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1362.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SG` | 211 | $6.43 | $2.72 | — | $8,188.59 | — | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.3; leftover $1362.14 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $6,843.00 | — | rank by candle_score; rank candle_score; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1362.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $5,581.58 | — | rank by candle_score; rank candle_score; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1362.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 82 | $16.46 | $2.24 | — | $4,229.63 | — | rank by candle_score; rank candle_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1362.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `TRLV` | 115 | $11.78 | $2.33 | — | $2,872.59 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1362.14 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 45 | $30.00 | $2.12 | — | $1,520.47 | — | rank by candle_score; rank candle_score; list ohlc_hot; ⚪; ret5=+14.1; leftover $1362.14 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 92 | $14.79 | $2.27 | — | $157.52 | — | rank by candle_score; rank candle_score; list yday_gainer; 🔵; ret5=+5.8; leftover $1362.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $157.52 | ▼ 09:30 equity $11,119.43 vs yday $11,144.82 (-25.39) | 09:30 open · cash $157.52 (unchanged overnight, no fees) · equity $11,119.43 vs prior close $11,144.82 (-25.39) because holdings re-marked: OMER×71 yday $18.86 → 09:30 $18.99 +9.23; SG×211 yday $6.73 → 09:30 $6.75 +4.22; ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; ARCT×82 yday $16.74 → 09:30 $16.77 +2.46; TRLV×115 yday $11.69 → 09:30 $11.89 +23.00; ZYME×45 yday $31.05 → 09:30 $31.34 +13.05; CLYM×92 yday $15.05 → 09:30 $13.96 -100.28 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $1,479.98 | ▲ +61.04 after sell → book $11,117.39; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 82 | $16.77 | $2.26 | $+20.92 | $2,852.86 | ▲ +20.92 after sell → book $11,115.13; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 92 | $13.96 | $2.29 | $-80.92 | $4,134.89 | ▼ -80.92 after sell → book $11,112.84; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 80 | $17.06 | $2.23 | — | $2,767.86 | — | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $1378.30 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 132 | $10.41 | $2.39 | — | $1,391.35 | — | rank by candle_score; rank candle_score; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1378.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `VIRT` | 21 | $63.37 | $2.05 | — | $58.53 | — | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+13.2; leftover $1378.30 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ADCT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CERS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYTX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OVID` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `KYMR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `MTDR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `PSKY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RDZN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBNX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMTX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SG` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `AVAH` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ZYME` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AVAH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `ZYME` | no_price | no 09:30 open |
| 2026-08-26 | `NVAX` | no_price | no 09:30 open |
| 2026-08-31 | `OMER` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WEN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ACB` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NOG` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `OMER` | 71 | 2026-09-03 @ $18.97 | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+12.1; leftover $1362.14 |
| `SG` | 211 | 2026-09-03 @ $6.43 | rank by candle_score; rank candle_score; list ohlc_hot; ret5=+11.3; leftover $1362.14 |
| `ATRC` | 27 | 2026-09-03 @ $49.76 | rank by candle_score; rank candle_score; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1362.14 |
| `TRLV` | 115 | 2026-09-03 @ $11.78 | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+15.0; leftover $1362.14 |
| `ZYME` | 45 | 2026-09-03 @ $30.00 | rank by candle_score; rank candle_score; list ohlc_hot; ⚪; ret5=+14.1; leftover $1362.14 |
| `HQ` | 80 | 2026-09-04 @ $17.06 | rank by candle_score; rank candle_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $1378.30 |
| `NVAX` | 132 | 2026-09-04 @ $10.41 | rank by candle_score; rank candle_score; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1378.30 |
| `VIRT` | 21 | 2026-09-04 @ $63.37 | rank by candle_score; rank candle_score; list ohlc_hot; 🔵; ret5=+13.2; leftover $1378.30 |
