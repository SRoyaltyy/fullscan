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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,161.33 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | BUY IREN x27 @ 45.98; BUY TNDM x53 @ 23.33; BUY TPG x24 @ 50.62; BUY INO x1543 @ 0.81; BUY HIMS x42 @ 29.74; BUY SLS x106 @ 11.70; BUY VOR x56 @ 22.01; BUY BTSG x20 @ 59.80 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $47.02 | $9,674.88 | $9,721.90 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, BZAI×1677, VOYG×28, LUNR×67 | SELL IREN (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); SELL BTSG (dropped from list after 1 sess (min 1)); BUY QMCO x52 @ 24.68; BUY ARX x65 @ 19.57; BUY ZENA x583 @ 2.20; BUY AIRO x115 @ 11.12; BUY LIFE x36 @ 35.04; BUY BZAI x1677 @ 0.77; BUY VOYG x28 @ 44.49; BUY LUNR x67 @ 19.17 |
| 2026-08-17 | +2.25 | $47.02 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, BZAI×1677, VOYG×28, LUNR×67 | XHG, CAPR, STDN, HTFL, UMAC, SMJF, ALOY, NPWR | QMCO, ARX, ZENA, AIRO, LIFE, BZAI, VOYG, LUNR | $33.54 | $9,303.42 | $9,336.96 | XHG×285, CAPR×174, STDN×87, HTFL×29, UMAC×36, SMJF×118, ALOY×81, NPWR×623 | SELL QMCO (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL ZENA (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL LIFE (dropped from list after 1 sess (min 1)); SELL BZAI (dropped from list after 1 sess (min 1)); SELL VOYG (dropped from list after 1 sess (min 1)); SELL LUNR (dropped from list after 1 sess (min 1)); BUY XHG x285 @ 4.19; BUY CAPR x174 @ 6.87; BUY STDN x87 @ 13.64; BUY HTFL x29 @ 41.23; BUY UMAC x36 @ 32.55; BUY SMJF x118 @ 10.10; BUY ALOY x81 @ 14.66; BUY NPWR x623 @ 1.92 |
| 2026-08-18 | -6.20 | $33.54 | XHG×285, CAPR×174, STDN×87, HTFL×29, UMAC×36, SMJF×118, ALOY×81, NPWR×623 | — | XHG, STDN, HTFL, UMAC, SMJF, ALOY, NPWR | $7,884.73 | $1,231.92 | $9,116.65 | CAPR×174 | SELL XHG (dropped from list after 1 sess (min 1)); SELL STDN (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); SELL UMAC (dropped from list after 1 sess (min 1)); SELL SMJF (dropped from list after 1 sess (min 1)); SELL ALOY (dropped from list after 1 sess (min 1)); SELL NPWR (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $7,884.73 | CAPR×174 | — | CAPR | $9,133.24 | $0.00 | $9,133.24 | — | SELL CAPR (dropped from list after 2 sess (min 1)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,133.24 | — | MRNA, CYPH, ABCL, AZI, SENS, ALEC, BTGO, AUTL | — | $63.54 | $8,898.14 | $8,961.68 | MRNA×7, CYPH×992, ABCL×96, AZI×833, SENS×128, ALEC×475, BTGO×172, AUTL×462 | BUY MRNA x7 @ 150.14; BUY CYPH x992 @ 1.15; BUY ABCL x96 @ 11.81; BUY AZI x833 @ 1.37; BUY SENS x128 @ 8.91; BUY ALEC x475 @ 2.40; BUY BTGO x172 @ 6.61; BUY AUTL x462 @ 2.47 |
| 2026-08-21 | +3.25 | $63.54 | MRNA×7, CYPH×992, ABCL×96, AZI×833, SENS×128, ALEC×475, BTGO×172, AUTL×462 | XHG, CAPR, ARCT, IOVA, CAN, TEM | ABCL, AZI, SENS, ALEC, BTGO, AUTL | $24.18 | $9,719.82 | $9,744.00 | MRNA×7, CYPH×992, XHG×258, CAPR×170, ARCT×104, IOVA×127, CAN×3946, TEM×17 | SELL ABCL (dropped from list after 1 sess (min 1)); SELL AZI (dropped from list after 1 sess (min 1)); SELL SENS (dropped from list after 1 sess (min 1)); SELL ALEC (dropped from list after 1 sess (min 1)); SELL BTGO (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); BUY XHG x258 @ 4.49; BUY CAPR x170 @ 6.81; BUY ARCT x104 @ 11.13; BUY IOVA x127 @ 9.08; BUY CAN x3946 @ 0.29; BUY TEM x17 @ 65.60 |
| 2026-08-24 | -5.17 | $24.18 | MRNA×7, CYPH×992, XHG×258, CAPR×170, ARCT×104, IOVA×127, CAN×3946, TEM×17 | — | MRNA, CYPH, XHG, CAPR, ARCT, IOVA, CAN, TEM | $10,330.90 | $0.00 | $10,330.90 | — | SELL MRNA (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL XHG (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL IOVA (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); SELL TEM (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,330.90 | — | CYPH, XHG, ASST, AU, RUM, BMNR, NIQ, DEFT | — | $86.74 | $10,037.28 | $10,124.02 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | BUY CYPH x759 @ 1.70; BUY XHG x321 @ 4.02; BUY ASST x61 @ 20.90; BUY AU x10 @ 119.46; BUY RUM x137 @ 9.36; BUY BMNR x52 @ 24.73; BUY NIQ x66 @ 19.56; BUY DEFT x2017 @ 0.64 |
| 2026-08-26 | +2.02 | $86.74 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | — | — | $86.74 | $10,200.34 | $10,287.08 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | hold CYPH,XHG,ASST,AU,RUM,BMNR,NIQ,DEFT |
| 2026-08-27 | — | $86.74 | CYPH×759, XHG×321, ASST×61, AU×10, RUM×137, BMNR×52, NIQ×66, DEFT×2017 | MOS, DLO, SLI, MRVL, CRK, PLTR, RRC, GEN | CYPH, XHG, ASST, AU, RUM, BMNR, NIQ, DEFT | $180.54 | $9,982.22 | $10,162.76 | MOS×50, DLO×80, SLI×485, MRVL×5, CRK×89, PLTR×7, RRC×30, GEN×43 | SELL CYPH (dropped from list after 2 sess (min 1)); SELL XHG (dropped from list after 2 sess (min 1)); SELL ASST (dropped from list after 2 sess (min 1)); SELL AU (dropped from list after 2 sess (min 1)); SELL RUM (dropped from list after 2 sess (min 1)); SELL BMNR (dropped from list after 2 sess (min 1)); SELL NIQ (dropped from list after 2 sess (min 1)); SELL DEFT (dropped from list after 2 sess (min 1)); BUY MOS x50 @ 24.84; BUY DLO x80 @ 15.60; BUY SLI x485 @ 2.59; BUY MRVL x5 @ 240.00; BUY CRK x89 @ 14.09; BUY PLTR x7 @ 170.60; BUY RRC x30 @ 40.72; BUY GEN x43 @ 28.89 |
| 2026-08-28 | +0.75 | $180.54 | MOS×50, DLO×80, SLI×485, MRVL×5, CRK×89, PLTR×7, RRC×30, GEN×43 | FIGR, NIQ, ERO, TRLV, CVI, VIRT, TXG, GUTS | MOS, DLO, SLI, MRVL, CRK, PLTR, RRC, GEN | $156.78 | $10,040.71 | $10,197.49 | FIGR×33, NIQ×67, ERO×32, TRLV×111, CVI×31, VIRT×19, TXG×19, GUTS×1718 | SELL MOS (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL MRVL (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL PLTR (dropped from list after 1 sess (min 1)); SELL RRC (dropped from list after 1 sess (min 1)); SELL GEN (dropped from list after 1 sess (min 1)); BUY FIGR x33 @ 37.42; BUY NIQ x67 @ 18.79; BUY ERO x32 @ 39.20; BUY TRLV x111 @ 11.38; BUY CVI x31 @ 40.04; BUY VIRT x19 @ 65.42; BUY TXG x19 @ 64.10; BUY GUTS x1718 @ 0.74 |
| 2026-08-31 | -5.85 | $156.78 | FIGR×33, NIQ×67, ERO×32, TRLV×111, CVI×31, VIRT×19, TXG×19, GUTS×1718 | — | FIGR, ERO, TRLV, CVI, VIRT, TXG, GUTS | $8,775.35 | $1,286.40 | $10,061.75 | NIQ×67 | SELL FIGR (dropped from list after 1 sess (min 1)); SELL ERO (dropped from list after 1 sess (min 1)); SELL TRLV (dropped from list after 1 sess (min 1)); SELL CVI (dropped from list after 1 sess (min 1)); SELL VIRT (dropped from list after 1 sess (min 1)); SELL TXG (dropped from list after 1 sess (min 1)); SELL GUTS (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $8,775.35 | NIQ×67 | — | NIQ | $10,050.16 | $0.00 | $10,050.16 | — | SELL NIQ (dropped from list after 2 sess (min 1)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,050.16 | — | — | — | $10,050.16 | $0.00 | $10,050.16 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,050.16 | — | MRNA, XHG, ARCT, CAN, NIQ, DEFT, OMER, ERO | — | $19.12 | $9,851.57 | $9,870.69 | MRNA×8, XHG×351, ARCT×76, CAN×4187, NIQ×67, DEFT×1875, OMER×66, ERO×35 | BUY MRNA x8 @ 151.40; BUY XHG x351 @ 3.57; BUY ARCT x76 @ 16.46; BUY CAN x4187 @ 0.30; BUY NIQ x67 @ 18.60; BUY DEFT x1875 @ 0.67; BUY OMER x66 @ 18.97; BUY ERO x35 @ 35.62 |
| 2026-09-04 | — | $19.12 | MRNA×8, XHG×351, ARCT×76, CAN×4187, NIQ×67, DEFT×1875, OMER×66, ERO×35 | HQ, OABI, TRLV | MRNA, ARCT, CAN | $1.48 | $9,913.50 | $9,914.98 | XHG×351, NIQ×67, DEFT×1875, OMER×66, ERO×35, HQ×75, OABI×252, TRLV×108 | SELL MRNA (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); BUY HQ x75 @ 17.06; BUY OABI x252 @ 5.08; BUY TRLV x108 @ 11.89 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | rank by hot_score; rank hot_score; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,295.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,508.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $5,248.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,471.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,783.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $9,087.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,718.65 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $6,428.53 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $2,581.40 | rank by hot_score; rank hot_score; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VOYG` | 28 | $44.49 | $2.07 | — | $1,333.60 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+15.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $47.02 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,336.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $2,605.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $3,813.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,911.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,134.94 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $7,046.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VOYG` | 28 | $42.12 | $2.09 | $-70.53 | $8,223.33 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $9,577.87 | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 285 | $4.19 | $3.68 | — | $8,380.04 | rank by hot_score; rank hot_score; list yday_mover; ⚪; ret5=+291.8; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,182.15 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+62.6; leftover $1197.23 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 87 | $13.64 | $2.25 | — | $5,993.22 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,795.47 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+46.0; leftover $1197.23 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,621.57 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1197.23 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `SMJF` | 118 | $10.10 | $2.34 | — | $2,427.43 | rank by hot_score; rank hot_score; list mover_buy; ret5=+22.8; leftover $1197.23 | join🔴 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 81 | $14.66 | $2.23 | — | $1,237.74 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1197.23 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 623 | $1.92 | $8.04 | — | $33.54 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1197.23 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 285 | $3.94 | $3.73 | $-78.66 | $1,152.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 87 | $13.31 | $2.28 | $-33.24 | $2,308.40 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,509.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,536.93 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `SMJF` | 118 | $10.45 | $2.37 | $+36.58 | $5,767.65 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 81 | $13.19 | $2.26 | $-123.56 | $6,833.79 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 623 | $1.70 | $8.15 | $-153.25 | $7,884.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,133.24 | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $8,080.25 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1141.66 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 992 | $1.15 | $12.80 | — | $6,926.66 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 96 | $11.81 | $2.28 | — | $5,790.14 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AZI` | 833 | $1.37 | $10.75 | — | $4,638.18 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $1141.66 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 128 | $8.91 | $2.37 | — | $3,495.33 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1141.66 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ALEC` | 475 | $2.40 | $6.13 | — | $2,349.20 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+13.0; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BTGO` | 172 | $6.61 | $2.51 | — | $1,210.64 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $1141.66 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 462 | $2.47 | $5.96 | — | $63.54 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1141.66 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 96 | $11.57 | $2.30 | $-28.10 | $1,171.95 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AZI` | 833 | $1.46 | $10.89 | $+53.33 | $2,377.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 128 | $9.24 | $2.41 | $+37.46 | $3,557.55 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ALEC` | 475 | $2.28 | $6.22 | $-69.34 | $4,634.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTGO` | 172 | $6.95 | $2.54 | $+54.29 | $5,827.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 462 | $2.47 | $6.05 | $-12.01 | $6,962.28 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 258 | $4.49 | $3.33 | — | $5,800.54 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+12.7; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 170 | $6.81 | $2.50 | — | $4,640.34 | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+62.5; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 104 | $11.13 | $2.30 | — | $3,480.51 | rank by hot_score; rank hot_score; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 127 | $9.08 | $2.37 | — | $2,324.98 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAN` | 3946 | $0.29 | $23.44 | — | $1,141.42 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $1160.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `TEM` | 17 | $65.60 | $2.04 | — | $24.18 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $1160.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,021.05 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 992 | $1.83 | $12.98 | $+648.79 | $2,823.43 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 258 | $4.24 | $3.38 | $-71.21 | $3,913.97 | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 170 | $8.01 | $2.54 | $+198.96 | $5,273.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 104 | $13.26 | $2.33 | $+216.89 | $6,649.84 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 127 | $8.05 | $2.40 | $-135.58 | $7,669.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAN` | 3946 | $0.38 | $27.50 | $+288.42 | $9,141.77 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 17 | $70.07 | $2.06 | $+71.89 | $10,330.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 759 | $1.70 | $9.79 | — | $9,030.81 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 321 | $4.02 | $4.14 | — | $7,736.25 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.1; leftover $1291.36 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 61 | $20.90 | $2.17 | — | $6,459.17 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+47.9; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $5,262.55 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RUM` | 137 | $9.36 | $2.40 | — | $3,977.83 | rank by hot_score; rank hot_score; list yday_gainer; 🔵; ret5=+21.3; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 52 | $24.73 | $2.15 | — | $2,689.73 | rank by hot_score; rank hot_score; list yday_gainer; ret5=+26.3; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NIQ` | 66 | $19.56 | $2.19 | — | $1,396.58 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1291.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2017 | $0.64 | $18.96 | — | $86.74 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1291.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 759 | $1.60 | $9.93 | $-95.62 | $1,291.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 321 | $3.81 | $4.20 | $-75.76 | $2,510.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 61 | $20.72 | $2.19 | $-15.35 | $3,771.74 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $4,967.70 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RUM` | 137 | $10.07 | $2.43 | $+92.43 | $6,344.86 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 52 | $24.24 | $2.17 | $-29.79 | $7,603.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NIQ` | 66 | $19.20 | $2.21 | $-28.16 | $8,868.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 2017 | $0.60 | $18.50 | $-118.14 | $10,059.87 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 50 | $24.84 | $2.14 | — | $8,815.73 | rank by hot_score; rank hot_score; list flatten; ret5=+13.0; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 80 | $15.60 | $2.23 | — | $7,565.50 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+7.1; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 485 | $2.59 | $6.26 | — | $6,303.09 | rank by hot_score; rank hot_score; list flatten; ret5=+4.2; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $5,101.09 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+6.8; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 89 | $14.09 | $2.26 | — | $3,844.82 | rank by hot_score; rank hot_score; list flatten; ret5=+1.1; leftover $1257.48 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $2,648.61 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+3.4; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 30 | $40.72 | $2.08 | — | $1,424.93 | rank by hot_score; rank hot_score; list flatten; ret5=+1.8; leftover $1257.48 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 43 | $28.89 | $2.12 | — | $180.54 | rank by hot_score; rank hot_score; list mover_buy; 🔵; ret5=+1.6; leftover $1257.48 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 50 | $24.00 | $2.16 | $-46.30 | $1,378.38 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 80 | $15.33 | $2.25 | $-26.08 | $2,602.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 485 | $2.60 | $6.35 | $-7.75 | $3,857.18 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $5,122.35 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 89 | $14.42 | $2.28 | $+24.83 | $6,403.45 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $7,652.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 30 | $41.44 | $2.10 | $+17.42 | $8,893.77 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 43 | $29.83 | $2.14 | $+36.16 | $10,174.32 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.42 | $2.09 | — | $8,937.37 | rank by hot_score; rank hot_score; list yday_mover; ret5=+24.4; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 67 | $18.79 | $2.19 | — | $7,676.25 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+7.6; leftover $1271.79 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 32 | $39.20 | $2.09 | — | $6,419.76 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.6; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `TRLV` | 111 | $11.38 | $2.32 | — | $5,154.26 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+15.0; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CVI` | 31 | $40.04 | $2.08 | — | $3,910.94 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 19 | $65.42 | $2.05 | — | $2,665.91 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+13.2; leftover $1271.79 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TXG` | 19 | $64.10 | $2.05 | — | $1,445.96 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GUTS` | 1718 | $0.74 | $17.87 | — | $156.78 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+14.7; leftover $1271.79 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.50 | $2.11 | $-67.56 | $1,326.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 32 | $38.60 | $2.11 | $-23.39 | $2,559.26 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `TRLV` | 111 | $12.41 | $2.35 | $+109.65 | $3,934.42 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `CVI` | 31 | $41.76 | $2.10 | $+49.13 | $5,226.88 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `VIRT` | 19 | $66.39 | $2.07 | $+14.32 | $6,486.22 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `TXG` | 19 | $60.90 | $2.07 | $-64.91 | $7,641.25 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GUTS` | 1718 | $0.67 | $16.96 | $-155.09 | $8,775.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `NIQ` | 67 | $19.06 | $2.21 | $+13.69 | $10,050.16 | dropped from list after 2 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 8 | $151.40 | $2.01 | — | $8,836.95 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 351 | $3.57 | $4.53 | — | $7,579.35 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+16.1; leftover $1256.27 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 76 | $16.46 | $2.22 | — | $6,326.17 | rank by hot_score; rank hot_score; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4187 | $0.30 | $25.12 | — | $5,044.95 | rank by hot_score; rank hot_score; list yday_mover; 🔵; ret5=+54.3; leftover $1256.27 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NIQ` | 67 | $18.60 | $2.19 | — | $3,796.56 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+7.6; leftover $1256.27 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟡 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1875 | $0.67 | $18.19 | — | $2,522.12 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1256.27 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `OMER` | 66 | $18.97 | $2.19 | — | $1,267.91 | rank by hot_score; rank hot_score; list ohlc_hot; ret5=+12.1; leftover $1256.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ERO` | 35 | $35.62 | $2.10 | — | $19.12 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ret5=+16.6; leftover $1256.27 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 8 | $145.95 | $2.03 | $-47.65 | $1,184.68 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 76 | $16.77 | $2.24 | $+19.10 | $2,456.96 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4187 | $0.34 | $27.50 | $+114.85 | $3,853.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 75 | $17.06 | $2.21 | — | $2,571.32 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; ret5=+17.3; leftover $1284.35 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 252 | $5.08 | $3.25 | — | $1,287.91 | rank by hot_score; rank hot_score; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1284.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 108 | $11.89 | $2.31 | — | $1.48 | rank by hot_score; rank hot_score; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1284.35 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

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
