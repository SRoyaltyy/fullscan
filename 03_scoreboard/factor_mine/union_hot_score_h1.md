# Factor mine action — `union_hot_score_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `hot_score` · size `leftover` · sell `list` · S-boost `none` · rank by hot_score

Cash book **-0.85%** ($9,915) · signal-only (no cash/fees) was -0.76%. Starts YES **5/17**. Fills 138 · skips 56 · realized $+136.47.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `hot_score` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $1.48.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | $10,312.70 | +43.99 | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $47.02 | $9,721.90 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, BZAI×1677, VOYG×28, LUNR×67 | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 |
| 2026-08-17 | +2.25 | $47.02 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, BZAI×1677, VOYG×28, LUNR×67 | $9,613.23 | -108.67 | XHG, CAPR, STDN, HTFL, UMAC, SMJF, ALOY, NPWR | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR | $33.54 | $9,336.96 | XHG×285, CAPR×174, STDN×87, HTFL×29, UMAC×36, SMJF×118, ALOY×81, NPWR×623 | 09:30 open · cash $47.02 (unchanged overnight, no fees) · equity $9,613.23 vs prior close $9,721.90 (-108.67) because holdings re-marked: QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; BZAI×1677 yday $0.59 → 09:30 $0.55 -68.76; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08 |
| 2026-08-18 | -6.20 | $33.54 | XHG×285, CAPR×174, STDN×87, HTFL×29, UMAC×36, SMJF×118, ALOY×81, NPWR×623 | $9,212.74 | -124.22 | — | XHG, STDN, HTFL, UMAC, SMJF, ALOY, NPWR | $7,884.73 | $9,116.65 | CAPR×174 | 09:30 open · cash $33.54 (unchanged overnight, no fees) · equity $9,212.74 vs prior close $9,336.96 (-124.22) because holdings re-marked: XHG×285 yday $3.91 → 09:30 $3.94 +8.55; CAPR×174 yday $7.45 → 09:30 $7.50 +8.70; STDN×87 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×36 yday $30.15 → 09:30 $28.59 -56.16; SMJF×118 yday $10.45 → 09:30 $10.45 +0.00; ALOY×81 yday $13.86 → 09:30 $13.19 -53.87; NPWR×623 yday $1.73 → 09:30 $1.70 -18.69 |
| 2026-08-19 | -7.20 | $7,884.73 | CAPR×174 | $9,135.79 | +19.14 | — | CAPR | $9,133.24 | $9,133.24 | — | 09:30 open · cash $7,884.73 (unchanged overnight, no fees) · equity $9,135.79 vs prior close $9,116.65 (+19.14) because holdings re-marked: CAPR×174 yday $7.08 → 09:30 $7.19 +19.14 |
| 2026-08-20 | +1.12 | $9,133.24 | — | $9,133.24 | +0.00 | MRNA, CYPH, ABCL, AZI, SENS, ALEC, BTGO, AUTL | — | $63.54 | $8,961.68 | MRNA×7, CYPH×992, ABCL×96, AZI×833, SENS×128, ALEC×475, BTGO×172, AUTL×462 | 09:30 open · cash $9,133.24 · no holdings · equity $9,133.24 vs prior close $9,133.24 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $63.54 | MRNA×7, CYPH×992, ABCL×96, AZI×833, SENS×128, ALEC×475, BTGO×172, AUTL×462 | $9,233.91 | +272.23 | XHG, CAPR, ARCT, IOVA, CAN, TEM | ABCL, AZI, SENS, ALEC, BTGO, AUTL | $24.18 | $9,744.00 | MRNA×7, CYPH×992, XHG×258, CAPR×170, ARCT×104, IOVA×127, CAN×3946, TEM×17 | 09:30 open · cash $63.54 (unchanged overnight, no fees) · equity $9,233.91 vs prior close $8,961.68 (+272.23) because holdings re-marked: MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×992 yday $1.19 → 09:30 $1.32 +128.96; ABCL×96 yday $11.57 → 09:30 $11.57 +0.00; AZI×833 yday $1.44 → 09:30 $1.46 +16.66; SENS×128 yday $8.82 → 09:30 $9.24 +53.76; ALEC×475 yday $2.26 → 09:30 $2.28 +9.50; BTGO×172 yday $6.60 → 09:30 $6.95 +60.20; AUTL×462 yday $2.46 → 09:30 $2.47 +4.62 |
| 2026-08-24 | -5.17 | $24.18 | MRNA×7, CYPH×992, XHG×258, CAPR×170, ARCT×104, IOVA×127, CAN×3946, TEM×17 | $10,386.12 | +642.12 | — | MRNA, CYPH, XHG, CAPR, ARCT, IOVA, CAN, TEM | $10,330.90 | $10,330.90 | — | 09:30 open · cash $24.18 (unchanged overnight, no fees) · equity $10,386.12 vs prior close $9,744.00 (+642.12) because holdings re-marked: MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×992 yday $1.42 → 09:30 $1.83 +406.72; XHG×258 yday $4.41 → 09:30 $4.24 -43.86; CAPR×170 yday $6.29 → 09:30 $8.01 +292.40; ARCT×104 yday $13.45 → 09:30 $13.26 -19.76; IOVA×127 yday $8.29 → 09:30 $8.05 -30.48; CAN×3946 yday $0.35 → 09:30 $0.38 +98.65; TEM×17 yday $72.69 → 09:30 $70.07 -44.54 |
| 2026-08-25 | +1.80 | $10,330.90 | — | $10,330.90 | -0.00 | CYPH, XHG, ASST, AU, RUM, BMNR, NIQ, DEFT | — | $86.74 | $10,124.02 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | 09:30 open · cash $10,330.90 · no holdings · equity $10,330.90 vs prior close $10,330.90 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $86.74 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | $10,124.02 | -0.00 | — | — | $86.74 | $10,287.08 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | 09:30 open · cash $86.74 (unchanged overnight, no fees) · equity $10,124.02 vs prior close $10,124.02 (-0.00) because holdings re-marked: CYPH×759 yday $1.64 → 09:30 $1.64 +0.00; XHG×321 yday $4.05 → 09:30 $4.05 +0.00; ASST×61 yday $20.20 → 09:30 $20.20 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; RUM×137 yday $9.35 → 09:30 $9.35 +0.00; BMNR×52 yday $24.21 → 09:30 $24.21 +0.00; NIQ×66 yday $19.46 → 09:30 $19.46 +0.00; DEFT×2017 yday $0.62 → 09:30 $0.62 +0.00 |
| 2026-08-27 | — | $86.74 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | $10,103.54 | -183.54 | MOS, DLO, SLI, MRVL, CRK, PLTR, RRC, GEN | CYPH, XHG, ASST, AU, RUM, BMNR, NIQ, DEFT | $180.54 | $10,162.76 | MOS×50, DLO×80, SLI×485, MRVL×5, CRK×89, PLTR×7, RRC×30, GEN×43 | 09:30 open · cash $86.74 (unchanged overnight, no fees) · equity $10,103.54 vs prior close $10,287.08 (-183.54) because holdings re-marked: CYPH×759 yday $1.64 → 09:30 $1.60 -30.36; XHG×321 yday $4.05 → 09:30 $3.81 -77.04; ASST×61 yday $20.20 → 09:30 $20.72 +31.72; AU×10 yday $118.55 → 09:30 $119.80 +12.50; RUM×137 yday $9.35 → 09:30 $10.07 +98.64; BMNR×52 yday $24.21 → 09:30 $24.24 +1.56; NIQ×66 yday $19.46 → 09:30 $19.20 -17.16; DEFT×2017 yday $0.62 → 09:30 $0.60 -40.34 |
| 2026-08-28 | +0.75 | $180.54 | MOS×50, DLO×80, SLI×485, MRVL×5, CRK×89, PLTR×7, RRC×30, GEN×43 | $10,195.66 | +32.90 | FIGR, NIQ, ERO, TRLV, CVI, VIRT, TXG, GUTS | MOS, DLO, SLI, MRVL, CRK, PLTR, RRC, GEN | $156.78 | $10,197.49 | FIGR×33, NIQ×67, ERO×32, TRLV×111, CVI×31, VIRT×19, TXG×19, GUTS×1718 | 09:30 open · cash $180.54 (unchanged overnight, no fees) · equity $10,195.66 vs prior close $10,162.76 (+32.90) because holdings re-marked: MOS×50 yday $24.16 → 09:30 $24.00 -8.00; DLO×80 yday $15.36 → 09:30 $15.33 -2.40; SLI×485 yday $2.61 → 09:30 $2.60 -4.85; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; CRK×89 yday $14.50 → 09:30 $14.42 -7.12; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; RRC×30 yday $41.55 → 09:30 $41.44 -3.30; GEN×43 yday $29.64 → 09:30 $29.83 +8.17 |
| 2026-08-31 | -5.85 | $156.78 | FIGR×33, NIQ×67, ERO×32, TRLV×111, CVI×31, VIRT×19, TXG×19, GUTS×1718 | $10,091.52 | -105.97 | — | FIGR, ERO, TRLV, CVI, VIRT, TXG, GUTS | $8,775.35 | $10,061.75 | NIQ×67 | 09:30 open · cash $156.78 (unchanged overnight, no fees) · equity $10,091.52 vs prior close $10,197.49 (-105.97) because holdings re-marked: FIGR×33 yday $38.02 → 09:30 $35.50 -83.16; NIQ×67 yday $19.07 → 09:30 $19.20 +8.71; ERO×32 yday $39.82 → 09:30 $38.60 -39.04; TRLV×111 yday $11.03 → 09:30 $12.41 +153.18; CVI×31 yday $39.76 → 09:30 $41.76 +62.00; VIRT×19 yday $67.04 → 09:30 $66.39 -12.35; TXG×19 yday $64.85 → 09:30 $60.90 -75.05; GUTS×1718 yday $0.74 → 09:30 $0.67 -120.26 |
| 2026-09-01 | -6.30 | $8,775.35 | NIQ×67 | $10,052.37 | -9.38 | — | NIQ | $10,050.16 | $10,050.16 | — | 09:30 open · cash $8,775.35 (unchanged overnight, no fees) · equity $10,052.37 vs prior close $10,061.75 (-9.38) because holdings re-marked: NIQ×67 yday $19.20 → 09:30 $19.06 -9.38 |
| 2026-09-02 | -3.83 | $10,050.16 | — | $10,050.16 | +0.00 | — | — | $10,050.16 | $10,050.16 | — | 09:30 open · cash $10,050.16 · no holdings · equity $10,050.16 vs prior close $10,050.16 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,050.16 | — | $10,050.16 | +0.00 | MRNA, XHG, ARCT, CAN, NIQ, DEFT, OMER, ERO | — | $19.12 | $9,870.69 | MRNA×8, XHG×351, ARCT×76, CAN×4187, NIQ×67, DEFT×1875, OMER×66, ERO×35 | 09:30 open · cash $10,050.16 · no holdings · equity $10,050.16 vs prior close $10,050.16 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $19.12 | MRNA×8, XHG×351, ARCT×76, CAN×4187, NIQ×67, DEFT×1875, OMER×66, ERO×35 | $10,047.21 | +176.52 | HQ, OABI, TRLV | MRNA, ARCT, CAN | $1.48 | $9,914.98 | XHG×351, NIQ×67, DEFT×1875, OMER×66, ERO×35, HQ×75, OABI×252, TRLV×108 | 09:30 open · cash $19.12 (unchanged overnight, no fees) · equity $10,047.21 vs prior close $9,870.69 (+176.52) because holdings re-marked: MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; XHG×351 yday $3.32 → 09:30 $3.38 +21.06; ARCT×76 yday $16.74 → 09:30 $16.77 +2.28; CAN×4187 yday $0.31 → 09:30 $0.34 +125.61; NIQ×67 yday $18.35 → 09:30 $18.66 +20.77; DEFT×1875 yday $0.65 → 09:30 $0.65 +0.00; OMER×66 yday $18.86 → 09:30 $18.99 +8.58; ERO×35 yday $34.76 → 09:30 $35.82 +37.10 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | — | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $107.38 | ▲ 09:30 equity $10,312.70 vs yday $10,268.71 (+43.99) | 09:30 open · cash $107.38 (unchanged overnight, no fees) · equity $10,312.70 vs prior close $10,268.71 (+43.99) because holdings re-marked: IREN×27 yday $44.76 → 09:30 $44.09 -18.09; TNDM×53 yday $23.13 → 09:30 $22.92 -11.13; TPG×24 yday $54.62 → 09:30 $55.29 +16.08; INO×1543 yday $0.90 → 09:30 $0.93 +46.29; HIMS×42 yday $28.77 → 09:30 $29.15 +15.96; SLS×106 yday $12.36 → 09:30 $12.40 +4.24; VOR×56 yday $23.29 → 09:30 $23.33 +2.24; BTSG×20 yday $60.23 → 09:30 $59.65 -11.60 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,295.72 | ▼ -55.19 after sell → book $10,310.61; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,508.31 | ▼ -26.05 after sell → book $10,308.44; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ +107.86 after sell → book $10,306.36; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $5,248.93 | ▲ +148.79 after sell → book $10,287.11; vs 09:30 mark -19.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,471.10 | ▼ -29.03 after sell → book $10,284.98; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,783.16 | ▲ +69.56 after sell → book $10,282.64; vs 09:30 mark -2.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $9,087.46 | ▲ +69.58 after sell → book $10,280.46; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▼ -7.12 after sell → book $10,278.39; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,718.65 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $6,428.53 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $2,581.40 | — | rank by hot_score; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $1,333.60 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $47.02 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.02 | ▼ 09:30 equity $9,613.23 vs yday $9,721.90 (-108.67) | 09:30 open · cash $47.02 (unchanged overnight, no fees) · equity $9,613.23 vs prior close $9,721.90 (-108.67) because holdings re-marked: QMCO×52 yday $26.11 → 09:30 $24.83 -66.56; ARX×65 yday $19.58 → 09:30 $19.57 -0.65; ZENA×583 yday $2.14 → 09:30 $2.08 -32.07; AIRO×115 yday $9.57 → 09:30 $9.57 +0.00; LIFE×36 yday $34.02 → 09:30 $34.03 +0.36; BZAI×1677 yday $0.59 → 09:30 $0.55 -68.76; VOYG×28 yday $42.98 → 09:30 $42.12 -24.08; LUNR×67 yday $19.01 → 09:30 $20.25 +83.08 | — |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,336.02 | ▲ +3.49 after sell → book $9,611.07; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $2,605.86 | ▼ -4.39 after sell → book $9,608.86; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $3,813.79 | ▼ -82.19 after sell → book $9,601.23; vs 09:30 mark -7.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,911.97 | ▼ -182.95 after sell → book $9,598.87; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,134.94 | ▼ -40.58 after sell → book $9,596.75; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $7,046.06 | ▼ -391.33 after sell → book $9,582.17; vs 09:30 mark -14.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $8,223.33 | ▼ -70.53 after sell → book $9,580.08; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $9,577.87 | ▲ +67.96 after sell → book $9,577.87; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 285 | $4.19 | $3.68 | — | $8,380.04 | — | rank by hot_score; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,182.15 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $1197.23 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 87 | $13.64 | $2.25 | — | $5,993.22 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,795.47 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $1197.23 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,621.57 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1197.23 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 118 | $10.10 | $2.34 | — | $2,427.43 | — | rank by hot_score; rank hot_score; list mover_buy; ret5=+22.8; leftover $1197.23 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 81 | $14.66 | $2.23 | — | $1,237.74 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 623 | $1.92 | $8.04 | — | $33.54 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1197.23 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $33.54 | ▼ 09:30 equity $9,212.74 vs yday $9,336.96 (-124.22) | 09:30 open · cash $33.54 (unchanged overnight, no fees) · equity $9,212.74 vs prior close $9,336.96 (-124.22) because holdings re-marked: XHG×285 yday $3.91 → 09:30 $3.94 +8.55; CAPR×174 yday $7.45 → 09:30 $7.50 +8.70; STDN×87 yday $13.31 → 09:30 $13.31 +0.00; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×36 yday $30.15 → 09:30 $28.59 -56.16; SMJF×118 yday $10.45 → 09:30 $10.45 +0.00; ALOY×81 yday $13.86 → 09:30 $13.19 -53.87; NPWR×623 yday $1.73 → 09:30 $1.70 -18.69 | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 285 | $3.94 | $3.73 | $-78.66 | $1,152.71 | ▼ -78.66 after sell → book $9,209.01; vs 09:30 mark -3.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 87 | $13.31 | $2.28 | $-33.24 | $2,308.40 | ▼ -33.24 after sell → book $9,206.73; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,509.80 | ▲ +3.66 after sell → book $9,204.63; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,536.93 | ▼ -146.78 after sell → book $9,202.51; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 118 | $10.45 | $2.37 | $+36.58 | $5,767.65 | ▲ +36.58 after sell → book $9,200.14; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 81 | $13.19 | $2.26 | $-123.56 | $6,833.79 | ▼ -123.56 after sell → book $9,197.89; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 623 | $1.70 | $8.15 | $-153.25 | $7,884.73 | ▼ -153.25 after sell → book $9,189.73; vs 09:30 mark -8.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7,884.73 | ▲ 09:30 equity $9,135.79 vs yday $9,116.65 (+19.14) | 09:30 open · cash $7,884.73 (unchanged overnight, no fees) · equity $9,135.79 vs prior close $9,116.65 (+19.14) because holdings re-marked: CAPR×174 yday $7.08 → 09:30 $7.19 +19.14 | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,133.24 | ▲ +50.62 after sell → book $9,133.24; vs 09:30 mark -2.55 | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,133.24 | ▲ 09:30 equity $9,133.24 vs yday $9,133.24 (+0.00) | 09:30 open · cash $9,133.24 · no holdings · equity $9,133.24 vs prior close $9,133.24 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,080.25 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1141.66 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 992 | $1.15 | $12.80 | — | $6,926.66 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 96 | $11.81 | $2.28 | — | $5,790.14 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 833 | $1.37 | $10.75 | — | $4,638.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1141.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 128 | $8.91 | $2.37 | — | $3,495.33 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1141.66 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 475 | $2.40 | $6.13 | — | $2,349.20 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 172 | $6.61 | $2.51 | — | $1,210.64 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1141.66 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 462 | $2.47 | $5.96 | — | $63.54 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.54 | ▲ 09:30 equity $9,233.91 vs yday $8,961.68 (+272.23) | 09:30 open · cash $63.54 (unchanged overnight, no fees) · equity $9,233.91 vs prior close $8,961.68 (+272.23) because holdings re-marked: MRNA×7 yday $133.32 → 09:30 $133.11 -1.47; CYPH×992 yday $1.19 → 09:30 $1.32 +128.96; ABCL×96 yday $11.57 → 09:30 $11.57 +0.00; AZI×833 yday $1.44 → 09:30 $1.46 +16.66; SENS×128 yday $8.82 → 09:30 $9.24 +53.76; ALEC×475 yday $2.26 → 09:30 $2.28 +9.50; BTGO×172 yday $6.60 → 09:30 $6.95 +60.20; AUTL×462 yday $2.46 → 09:30 $2.47 +4.62 | — |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 96 | $11.57 | $2.30 | $-28.10 | $1,171.95 | ▼ -28.10 after sell → book $9,231.60; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 833 | $1.46 | $10.89 | $+53.33 | $2,377.24 | ▲ +53.33 after sell → book $9,220.71; vs 09:30 mark -10.89 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 128 | $9.24 | $2.41 | $+37.46 | $3,557.55 | ▲ +37.46 after sell → book $9,218.30; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 475 | $2.28 | $6.22 | $-69.34 | $4,634.34 | ▼ -69.34 after sell → book $9,212.09; vs 09:30 mark -6.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 172 | $6.95 | $2.54 | $+54.29 | $5,827.19 | ▲ +54.29 after sell → book $9,209.54; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 462 | $2.47 | $6.05 | $-12.01 | $6,962.28 | ▼ -12.01 after sell → book $9,203.49; vs 09:30 mark -6.05 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 258 | $4.49 | $3.33 | — | $5,800.54 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 170 | $6.81 | $2.50 | — | $4,640.34 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $3,480.51 | — | rank by hot_score; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 127 | $9.08 | $2.37 | — | $2,324.98 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 3946 | $0.29 | $23.44 | — | $1,141.42 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1160.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 17 | $65.60 | $2.04 | — | $24.18 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24.18 | ▲ 09:30 equity $10,386.12 vs yday $9,744.00 (+642.12) | 09:30 open · cash $24.18 (unchanged overnight, no fees) · equity $10,386.12 vs prior close $9,744.00 (+642.12) because holdings re-marked: MRNA×7 yday $145.13 → 09:30 $142.70 -17.01; CYPH×992 yday $1.42 → 09:30 $1.83 +406.72; XHG×258 yday $4.41 → 09:30 $4.24 -43.86; CAPR×170 yday $6.29 → 09:30 $8.01 +292.40; ARCT×104 yday $13.45 → 09:30 $13.26 -19.76; IOVA×127 yday $8.29 → 09:30 $8.05 -30.48; CAN×3946 yday $0.35 → 09:30 $0.38 +98.65; TEM×17 yday $72.69 → 09:30 $70.07 -44.54 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,021.05 | ▼ -56.12 after sell → book $10,384.09; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 992 | $1.83 | $12.98 | $+648.79 | $2,823.43 | ▲ +648.79 after sell → book $10,371.11; vs 09:30 mark -12.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 258 | $4.24 | $3.38 | $-71.21 | $3,913.97 | ▼ -71.21 after sell → book $10,367.73; vs 09:30 mark -3.38 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 170 | $8.01 | $2.54 | $+198.96 | $5,273.13 | ▲ +198.96 after sell → book $10,365.19; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.26 | $2.33 | $+216.89 | $6,649.84 | ▲ +216.89 after sell → book $10,362.86; vs 09:30 mark -2.33 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 127 | $8.05 | $2.40 | $-135.58 | $7,669.79 | ▼ -135.58 after sell → book $10,360.46; vs 09:30 mark -2.40 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 3946 | $0.38 | $27.50 | $+288.42 | $9,141.77 | ▲ +288.42 after sell → book $10,332.96; vs 09:30 mark -27.50 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 17 | $70.07 | $2.06 | $+71.89 | $10,330.90 | ▲ +71.89 after sell → book $10,330.90; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,330.90 | ▲ 09:30 equity $10,330.90 vs yday $10,330.90 (-0.00) | 09:30 open · cash $10,330.90 · no holdings · equity $10,330.90 vs prior close $10,330.90 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 759 | $1.70 | $9.79 | — | $9,030.81 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 321 | $4.02 | $4.14 | — | $7,736.25 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $1291.36 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 61 | $20.90 | $2.17 | — | $6,459.17 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+47.9; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $5,262.55 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 137 | $9.36 | $2.40 | — | $3,977.83 | — | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+21.3; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 52 | $24.73 | $2.15 | — | $2,689.73 | — | rank by hot_score; rank hot_score; list yday_gainer; ret5=+26.3; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 66 | $19.56 | $2.19 | — | $1,396.58 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2017 | $0.64 | $18.96 | — | $86.74 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.74 | ▲ 09:30 equity $10,124.02 vs yday $10,124.02 (-0.00) | 09:30 open · cash $86.74 (unchanged overnight, no fees) · equity $10,124.02 vs prior close $10,124.02 (-0.00) because holdings re-marked: CYPH×759 yday $1.64 → 09:30 $1.64 +0.00; XHG×321 yday $4.05 → 09:30 $4.05 +0.00; ASST×61 yday $20.20 → 09:30 $20.20 +0.00; AU×10 yday $118.55 → 09:30 $118.55 +0.00; RUM×137 yday $9.35 → 09:30 $9.35 +0.00; BMNR×52 yday $24.21 → 09:30 $24.21 +0.00; NIQ×66 yday $19.46 → 09:30 $19.46 +0.00; DEFT×2017 yday $0.62 → 09:30 $0.62 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $86.74 | ▼ 09:30 equity $10,103.54 vs yday $10,287.08 (-183.54) | 09:30 open · cash $86.74 (unchanged overnight, no fees) · equity $10,103.54 vs prior close $10,287.08 (-183.54) because holdings re-marked: CYPH×759 yday $1.64 → 09:30 $1.60 -30.36; XHG×321 yday $4.05 → 09:30 $3.81 -77.04; ASST×61 yday $20.20 → 09:30 $20.72 +31.72; AU×10 yday $118.55 → 09:30 $119.80 +12.50; RUM×137 yday $9.35 → 09:30 $10.07 +98.64; BMNR×52 yday $24.21 → 09:30 $24.24 +1.56; NIQ×66 yday $19.46 → 09:30 $19.20 -17.16; DEFT×2017 yday $0.62 → 09:30 $0.60 -40.34 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 759 | $1.60 | $9.93 | $-95.62 | $1,291.21 | ▼ -95.62 after sell → book $10,093.61; vs 09:30 mark -9.93 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 321 | $3.81 | $4.20 | $-75.76 | $2,510.02 | ▼ -75.76 after sell → book $10,089.41; vs 09:30 mark -4.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 61 | $20.72 | $2.19 | $-15.35 | $3,771.74 | ▼ -15.35 after sell → book $10,087.21; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $4,967.70 | ▼ -0.66 after sell → book $10,085.17; vs 09:30 mark -2.04 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 137 | $10.07 | $2.43 | $+92.43 | $6,344.86 | ▲ +92.43 after sell → book $10,082.74; vs 09:30 mark -2.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 52 | $24.24 | $2.17 | $-29.79 | $7,603.17 | ▼ -29.79 after sell → book $10,080.57; vs 09:30 mark -2.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 66 | $19.20 | $2.21 | $-28.16 | $8,868.16 | ▼ -28.16 after sell → book $10,078.36; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 2017 | $0.60 | $18.50 | $-118.14 | $10,059.87 | ▼ -118.14 after sell → book $10,059.87; vs 09:30 mark -18.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 50 | $24.84 | $2.14 | — | $8,815.73 | — | rank by hot_score; rank hot_score; list flatten; ret5=+13.0; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 80 | $15.60 | $2.23 | — | $7,565.50 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+7.1; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 485 | $2.59 | $6.26 | — | $6,303.09 | — | rank by hot_score; rank hot_score; list flatten; ret5=+4.2; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $5,101.09 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+6.8; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 89 | $14.09 | $2.26 | — | $3,844.82 | — | rank by hot_score; rank hot_score; list flatten; ret5=+1.1; leftover $1257.48 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $2,648.61 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+3.4; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $40.72 | $2.08 | — | $1,424.93 | — | rank by hot_score; rank hot_score; list flatten; ret5=+1.8; leftover $1257.48 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $28.89 | $2.12 | — | $180.54 | — | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+1.6; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $180.54 | ▲ 09:30 equity $10,195.66 vs yday $10,162.76 (+32.90) | 09:30 open · cash $180.54 (unchanged overnight, no fees) · equity $10,195.66 vs prior close $10,162.76 (+32.90) because holdings re-marked: MOS×50 yday $24.16 → 09:30 $24.00 -8.00; DLO×80 yday $15.36 → 09:30 $15.33 -2.40; SLI×485 yday $2.61 → 09:30 $2.60 -4.85; MRVL×5 yday $245.11 → 09:30 $253.44 +41.65; CRK×89 yday $14.50 → 09:30 $14.42 -7.12; PLTR×7 yday $177.50 → 09:30 $178.75 +8.75; RRC×30 yday $41.55 → 09:30 $41.44 -3.30; GEN×43 yday $29.64 → 09:30 $29.83 +8.17 | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 50 | $24.00 | $2.16 | $-46.30 | $1,378.38 | ▼ -46.30 after sell → book $10,193.50; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 80 | $15.33 | $2.25 | $-26.08 | $2,602.53 | ▼ -26.08 after sell → book $10,191.24; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 485 | $2.60 | $6.35 | $-7.75 | $3,857.18 | ▼ -7.75 after sell → book $10,184.90; vs 09:30 mark -6.34 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $5,122.35 | ▲ +63.17 after sell → book $10,182.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 89 | $14.42 | $2.28 | $+24.83 | $6,403.45 | ▲ +24.83 after sell → book $10,180.59; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $7,652.67 | ▲ +53.01 after sell → book $10,178.56; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 30 | $41.44 | $2.10 | $+17.42 | $8,893.77 | ▲ +17.42 after sell → book $10,176.46; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $29.83 | $2.14 | $+36.16 | $10,174.32 | ▲ +36.16 after sell → book $10,174.32; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.42 | $2.09 | — | $8,937.37 | — | rank by hot_score; rank hot_score; list yday_mover; ret5=+24.4; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 67 | $18.79 | $2.19 | — | $7,676.25 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $1271.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 32 | $39.20 | $2.09 | — | $6,419.76 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.6; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 111 | $11.38 | $2.32 | — | $5,154.26 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+15.0; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 31 | $40.04 | $2.08 | — | $3,910.94 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 19 | $65.42 | $2.05 | — | $2,665.91 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.2; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 19 | $64.10 | $2.05 | — | $1,445.96 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GUTS` | 1718 | $0.74 | $17.87 | — | $156.78 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+14.7; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $156.78 | ▼ 09:30 equity $10,091.52 vs yday $10,197.49 (-105.97) | 09:30 open · cash $156.78 (unchanged overnight, no fees) · equity $10,091.52 vs prior close $10,197.49 (-105.97) because holdings re-marked: FIGR×33 yday $38.02 → 09:30 $35.50 -83.16; NIQ×67 yday $19.07 → 09:30 $19.20 +8.71; ERO×32 yday $39.82 → 09:30 $38.60 -39.04; TRLV×111 yday $11.03 → 09:30 $12.41 +153.18; CVI×31 yday $39.76 → 09:30 $41.76 +62.00; VIRT×19 yday $67.04 → 09:30 $66.39 -12.35; TXG×19 yday $64.85 → 09:30 $60.90 -75.05; GUTS×1718 yday $0.74 → 09:30 $0.67 -120.26 | — |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.50 | $2.11 | $-67.56 | $1,326.17 | ▼ -67.56 after sell → book $10,089.41; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 32 | $38.60 | $2.11 | $-23.39 | $2,559.26 | ▼ -23.39 after sell → book $10,087.30; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 111 | $12.41 | $2.35 | $+109.65 | $3,934.42 | ▲ +109.65 after sell → book $10,084.95; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CVI` | 31 | $41.76 | $2.10 | $+49.13 | $5,226.88 | ▲ +49.13 after sell → book $10,082.85; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `VIRT` | 19 | $66.39 | $2.07 | $+14.32 | $6,486.22 | ▲ +14.32 after sell → book $10,080.78; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TXG` | 19 | $60.90 | $2.07 | $-64.91 | $7,641.25 | ▼ -64.91 after sell → book $10,078.71; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GUTS` | 1718 | $0.67 | $16.96 | $-155.09 | $8,775.35 | ▼ -155.09 after sell → book $10,061.75; vs 09:30 mark -16.96 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,775.35 | ▼ 09:30 equity $10,052.37 vs yday $10,061.75 (-9.38) | 09:30 open · cash $8,775.35 (unchanged overnight, no fees) · equity $10,052.37 vs prior close $10,061.75 (-9.38) because holdings re-marked: NIQ×67 yday $19.20 → 09:30 $19.06 -9.38 | — |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 67 | $19.06 | $2.21 | $+13.69 | $10,050.16 | ▲ +13.69 after sell → book $10,050.16; vs 09:30 mark -2.21 | dropped from list after 2 sess (min 1) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,050.16 | ▲ 09:30 equity $10,050.16 vs yday $10,050.16 (+0.00) | 09:30 open · cash $10,050.16 · no holdings · equity $10,050.16 vs prior close $10,050.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,050.16 | ▲ 09:30 equity $10,050.16 vs yday $10,050.16 (+0.00) | 09:30 open · cash $10,050.16 · no holdings · equity $10,050.16 vs prior close $10,050.16 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $8,836.95 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 351 | $3.57 | $4.53 | — | $7,579.35 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1256.27 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.46 | $2.22 | — | $6,326.17 | — | rank by hot_score; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4187 | $0.30 | $25.12 | — | $5,044.95 | — | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $1256.27 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 67 | $18.60 | $2.19 | — | $3,796.56 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1256.27 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1875 | $0.67 | $18.19 | — | $2,522.12 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1256.27 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 66 | $18.97 | $2.19 | — | $1,267.91 | — | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ERO` | 35 | $35.62 | $2.10 | — | $19.12 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $1256.27 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.12 | ▲ 09:30 equity $10,047.21 vs yday $9,870.69 (+176.52) | 09:30 open · cash $19.12 (unchanged overnight, no fees) · equity $10,047.21 vs prior close $9,870.69 (+176.52) because holdings re-marked: MRNA×8 yday $150.81 → 09:30 $145.95 -38.88; XHG×351 yday $3.32 → 09:30 $3.38 +21.06; ARCT×76 yday $16.74 → 09:30 $16.77 +2.28; CAN×4187 yday $0.31 → 09:30 $0.34 +125.61; NIQ×67 yday $18.35 → 09:30 $18.66 +20.77; DEFT×1875 yday $0.65 → 09:30 $0.65 +0.00; OMER×66 yday $18.86 → 09:30 $18.99 +8.58; ERO×35 yday $34.76 → 09:30 $35.82 +37.10 | — |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $1,184.68 | ▼ -47.65 after sell → book $10,045.17; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 76 | $16.77 | $2.24 | $+19.10 | $2,456.96 | ▲ +19.10 after sell → book $10,042.93; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4187 | $0.34 | $27.50 | $+114.85 | $3,853.04 | ▲ +114.85 after sell → book $10,015.43; vs 09:30 mark -27.50 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 75 | $17.06 | $2.21 | — | $2,571.32 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $1284.35 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 252 | $5.08 | $3.25 | — | $1,287.91 | — | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1284.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 108 | $11.89 | $2.31 | — | $1.48 | — | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1284.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OMER` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TRLV` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RUM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `OMER` | no_price | no 09:30 open |
| 2026-08-26 | `ERO` | no_price | no 09:30 open |
| 2026-08-26 | `TRLV` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DEFT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TXG` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `GUTS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `UEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TRLV` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GUTS` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `WPM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 351 | 2026-09-03 @ $3.57 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1256.27 |
| `NIQ` | 67 | 2026-09-03 @ $18.60 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1256.27 |
| `DEFT` | 1875 | 2026-09-03 @ $0.67 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1256.27 |
| `OMER` | 66 | 2026-09-03 @ $18.97 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1256.27 |
| `ERO` | 35 | 2026-09-03 @ $35.62 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $1256.27 |
| `HQ` | 75 | 2026-09-04 @ $17.06 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $1284.35 |
| `OABI` | 252 | 2026-09-04 @ $5.08 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1284.35 |
| `TRLV` | 108 | 2026-09-04 @ $11.89 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1284.35 |
