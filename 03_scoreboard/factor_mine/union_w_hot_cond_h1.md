# Factor mine action — `union_w_hot_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `w_hot_cond` · size `leftover` · sell `list` · S-boost `none` · rank by w_hot_cond

Cash book **-4.34%** ($9,566) · signal-only (no cash/fees) was +0.19%. Starts YES **4/17**. Fills 140 · skips 54 · realized $-208.59.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `w_hot_cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $33.22.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | — | $107.38 | $10,161.33 | $10,268.71 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | BUY IREN x27 @ 45.98; BUY TNDM x53 @ 23.33; BUY TPG x24 @ 50.62; BUY INO x1543 @ 0.81; BUY HIMS x42 @ 29.74; BUY SLS x106 @ 11.70; BUY VOR x56 @ 22.01; BUY BTSG x20 @ 59.80 |
| 2026-08-14 | +5.50 | $107.38 | IREN×27, TNDM×53, TPG×24, INO×1543, HIMS×42, SLS×106, VOR×56, BTSG×20 | QMCO, ARX, ZENA, AIRO, LIFE, LUNR, TBBB, BZAI | IREN, TNDM, TPG, INO, HIMS, SLS, VOR, BTSG | $23.43 | $9,713.98 | $9,737.41 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, LUNR×67, TBBB×26, BZAI×1677 | SELL IREN (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); SELL BTSG (dropped from list after 1 sess (min 1)); BUY QMCO x52 @ 24.68; BUY ARX x65 @ 19.57; BUY ZENA x583 @ 2.20; BUY AIRO x115 @ 11.12; BUY LIFE x36 @ 35.04; BUY LUNR x67 @ 19.17; BUY TBBB x26 @ 48.82; BUY BZAI x1677 @ 0.77 |
| 2026-08-17 | +2.25 | $23.43 | QMCO×52, ARX×65, ZENA×583, AIRO×115, LIFE×36, LUNR×67, TBBB×26, BZAI×1677 | XHG, CAPR, STDN, HTFL, UMAC, ALOY, NPWR, LPTH | QMCO, ARX, ZENA, AIRO, LIFE, LUNR, TBBB, BZAI | $37.73 | $9,275.01 | $9,312.74 | XHG×286, CAPR×174, STDN×88, HTFL×29, UMAC×36, ALOY×81, NPWR×625, LPTH×80 | SELL QMCO (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL ZENA (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL LIFE (dropped from list after 1 sess (min 1)); SELL LUNR (dropped from list after 1 sess (min 1)); SELL TBBB (dropped from list after 1 sess (min 1)); SELL BZAI (dropped from list after 1 sess (min 1)); BUY XHG x286 @ 4.19; BUY CAPR x174 @ 6.87; BUY STDN x88 @ 13.64; BUY HTFL x29 @ 41.23; BUY UMAC x36 @ 32.55; BUY ALOY x81 @ 14.66; BUY NPWR x625 @ 1.92; BUY LPTH x80 @ 14.94 |
| 2026-08-18 | -6.20 | $37.73 | XHG×286, CAPR×174, STDN×88, HTFL×29, UMAC×36, ALOY×81, NPWR×625, LPTH×80 | — | XHG, STDN, HTFL, UMAC, ALOY, NPWR, LPTH | $7,797.36 | $1,231.92 | $9,029.28 | CAPR×174 | SELL XHG (dropped from list after 1 sess (min 1)); SELL STDN (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); SELL UMAC (dropped from list after 1 sess (min 1)); SELL ALOY (dropped from list after 1 sess (min 1)); SELL NPWR (dropped from list after 1 sess (min 1)); SELL LPTH (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $7,797.36 | CAPR×174 | — | CAPR | $9,045.87 | $0.00 | $9,045.87 | — | SELL CAPR (dropped from list after 2 sess (min 1)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,045.87 | — | MRNA, CYPH, ABCL, SENS, AUTL, TEM, WPM, IAG | — | $215.49 | $8,857.65 | $9,073.14 | MRNA×7, CYPH×983, ABCL×95, SENS×126, AUTL×457, TEM×18, WPM×7, IAG×57 | BUY MRNA x7 @ 150.14; BUY CYPH x983 @ 1.15; BUY ABCL x95 @ 11.81; BUY SENS x126 @ 8.91; BUY AUTL x457 @ 2.47; BUY TEM x18 @ 61.83; BUY WPM x7 @ 144.54; BUY IAG x57 @ 19.63 |
| 2026-08-21 | +3.25 | $215.49 | MRNA×7, CYPH×983, ABCL×95, SENS×126, AUTL×457, TEM×18, WPM×7, IAG×57 | XHG, ARCT, IOVA, CAPR, AU | ABCL, SENS, AUTL, WPM, IAG | $107.24 | $9,530.13 | $9,637.37 | MRNA×7, CYPH×983, TEM×18, XHG×262, ARCT×105, IOVA×129, CAPR×172, AU×9 | SELL ABCL (dropped from list after 1 sess (min 1)); SELL SENS (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); BUY XHG x262 @ 4.49; BUY ARCT x105 @ 11.13; BUY IOVA x129 @ 9.08; BUY CAPR x172 @ 6.81; BUY AU x9 @ 119.43 |
| 2026-08-24 | -5.17 | $107.24 | MRNA×7, CYPH×983, TEM×18, XHG×262, ARCT×105, IOVA×129, CAPR×172, AU×9 | — | MRNA, CYPH, TEM, XHG, ARCT, IOVA, CAPR, AU | $10,140.43 | $0.00 | $10,140.43 | — | SELL MRNA (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL TEM (dropped from list after 2 sess (min 1)); SELL XHG (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL IOVA (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); SELL AU (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,140.43 | — | CYPH, XHG, AU, ERO, ASST, HMY, FCX, WPM | — | $266.72 | $9,752.29 | $10,019.01 | CYPH×745, XHG×315, AU×10, ERO×33, ASST×60, HMY×55, FCX×16, WPM×7 | BUY CYPH x745 @ 1.70; BUY XHG x315 @ 4.02; BUY AU x10 @ 119.46; BUY ERO x33 @ 38.00; BUY ASST x60 @ 20.90; BUY HMY x55 @ 22.65; BUY FCX x16 @ 77.90; BUY WPM x7 @ 160.00 |
| 2026-08-26 | +2.02 | $266.72 | CYPH×745, XHG×315, AU×10, ERO×33, ASST×60, HMY×55, FCX×16, WPM×7 | — | — | $266.72 | $9,847.55 | $10,114.27 | CYPH×745, XHG×315, AU×10, ERO×33, ASST×60, HMY×55, FCX×16, WPM×7 | hold CYPH,XHG,AU,ERO,ASST,HMY,FCX,WPM |
| 2026-08-27 | — | $266.72 | CYPH×745, XHG×315, AU×10, ERO×33, ASST×60, HMY×55, FCX×16, WPM×7 | MOS, SLI, DLO, TX, MRVL, MU, PLTR, MT | CYPH, XHG, AU, ERO, ASST, HMY, FCX, WPM | $537.29 | $9,511.83 | $10,049.12 | MOS×50, SLI×484, DLO×80, TX×22, MRVL×5, MU×1, PLTR×7, MT×16 | SELL CYPH (dropped from list after 2 sess (min 1)); SELL XHG (dropped from list after 2 sess (min 1)); SELL AU (dropped from list after 2 sess (min 1)); SELL ERO (dropped from list after 2 sess (min 1)); SELL ASST (dropped from list after 2 sess (min 1)); SELL HMY (dropped from list after 2 sess (min 1)); SELL FCX (dropped from list after 2 sess (min 1)); SELL WPM (dropped from list after 2 sess (min 1)); BUY MOS x50 @ 24.84; BUY SLI x484 @ 2.59; BUY DLO x80 @ 15.60; BUY TX x22 @ 55.20; BUY MRVL x5 @ 240.00; BUY MU x1 @ 925.74; BUY PLTR x7 @ 170.60; BUY MT x16 @ 75.12 |
| 2026-08-28 | +0.75 | $537.29 | MOS×50, SLI×484, DLO×80, TX×22, MRVL×5, MU×1, PLTR×7, MT×16 | ERO, FIGR, BKKT, FCX, QMCO, TIGR, NIQ, VIRT | MOS, SLI, DLO, TX, MRVL, MU, PLTR, MT | $63.59 | $9,988.65 | $10,052.24 | ERO×32, FIGR×33, BKKT×148, FCX×16, QMCO×53, TIGR×229, NIQ×67, VIRT×19 | SELL MOS (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)); SELL TX (dropped from list after 1 sess (min 1)); SELL MRVL (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); SELL PLTR (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); BUY ERO x32 @ 39.20; BUY FIGR x33 @ 37.42; BUY BKKT x148 @ 8.50; BUY FCX x16 @ 78.83; BUY QMCO x53 @ 23.50; BUY TIGR x229 @ 5.49; BUY NIQ x67 @ 18.79; BUY VIRT x19 @ 65.42 |
| 2026-08-31 | -5.85 | $63.59 | ERO×32, FIGR×33, BKKT×148, FCX×16, QMCO×53, TIGR×229, NIQ×67, VIRT×19 | — | ERO, FIGR, BKKT, FCX, QMCO, NIQ, VIRT | $8,492.45 | $1,147.29 | $9,639.74 | TIGR×229 | SELL ERO (dropped from list after 1 sess (min 1)); SELL FIGR (dropped from list after 1 sess (min 1)); SELL BKKT (dropped from list after 1 sess (min 1)); SELL FCX (dropped from list after 1 sess (min 1)); SELL QMCO (dropped from list after 1 sess (min 1)); SELL NIQ (dropped from list after 1 sess (min 1)); SELL VIRT (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $8,492.45 | TIGR×229 | — | TIGR | $9,639.03 | $0.00 | $9,639.03 | — | SELL TIGR (dropped from list after 2 sess (min 1)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $9,639.03 | — | — | — | $9,639.03 | $0.00 | $9,639.03 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,639.03 | — | MRNA, ARCT, XHG, CAN, NVAX, INO, RVTY, ZYME | — | $179.32 | $9,492.03 | $9,671.35 | MRNA×7, ARCT×73, XHG×337, CAN×4016, NVAX×117, INO×899, RVTY×9, ZYME×40 | BUY MRNA x7 @ 151.40; BUY ARCT x73 @ 16.46; BUY XHG x337 @ 3.57; BUY CAN x4016 @ 0.30; BUY NVAX x117 @ 10.27; BUY INO x899 @ 1.34; BUY RVTY x9 @ 125.94; BUY ZYME x40 @ 30.00 |
| 2026-09-04 | — | $179.32 | MRNA×7, ARCT×73, XHG×337, CAN×4016, NVAX×117, INO×899, RVTY×9, ZYME×40 | OABI, TRLV, ALEC, OMER, ATRC | MRNA, ARCT, CAN, NVAX, RVTY | $33.22 | $9,533.20 | $9,566.42 | XHG×337, INO×899, ZYME×40, OABI×242, TRLV×103, ALEC×456, OMER×64, ATRC×23 | SELL MRNA (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CAN (dropped from list after 1 sess (min 1)); SELL NVAX (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); BUY OABI x242 @ 5.08; BUY TRLV x103 @ 11.89; BUY ALEC x456 @ 2.70; BUY OMER x64 @ 18.99; BUY ATRC x23 @ 52.88 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $8,756.47 | ▼ $9,997.93 (-2.07) | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $7,517.83 | ▼ $9,995.78 (-4.22) | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,300.81 | ▼ $9,993.72 (-6.28) | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $5,033.85 | ▼ $9,976.59 (-23.41) | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $3,782.66 | ▼ $9,974.47 (-25.53) | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $2,540.15 | ▼ $9,972.17 (-27.83) | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 56 | $22.01 | $2.16 | — | $1,305.43 | ▼ $9,970.01 (-29.99) | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=+0.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $107.38 | ▼ $9,967.96 (-32.04) | rank by w_hot_cond; rank w_hot_cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $1,295.72 | ▲ $10,310.61 (+310.61) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $2,508.31 | ▲ $10,308.44 (+308.44) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,833.19 | ▲ $10,306.36 (+306.36) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $5,248.93 | ▲ $10,287.11 (+287.11) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $6,471.10 | ▲ $10,284.98 (+284.98) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $7,783.16 | ▲ $10,282.64 (+282.64) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 56 | $23.33 | $2.18 | $+69.58 | $9,087.46 | ▲ $10,280.46 (+280.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $10,278.39 | ▲ $10,278.39 (+278.39) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `QMCO` | 52 | $24.68 | $2.15 | — | $8,992.89 | ▲ $10,276.25 (+276.25) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+111.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 65 | $19.57 | $2.19 | — | $7,718.65 | ▲ $10,274.06 (+274.06) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ZENA` | 583 | $2.20 | $7.52 | — | $6,428.53 | ▲ $10,266.54 (+266.54) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+54.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 115 | $11.12 | $2.33 | — | $5,147.40 | ▲ $10,264.21 (+264.21) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LIFE` | 36 | $35.04 | $2.10 | — | $3,883.86 | ▲ $10,262.11 (+262.11) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.7; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LUNR` | 67 | $19.17 | $2.19 | — | $2,597.28 | ▲ $10,259.92 (+259.92) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🔴 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 26 | $48.82 | $2.07 | — | $1,325.89 | ▲ $10,257.85 (+257.85) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BZAI` | 1677 | $0.77 | $17.88 | — | $23.43 | ▲ $10,239.97 (+239.97) | rank by w_hot_cond; rank w_hot_cond; list earn_react; 🔵; ⚪; ret5=+20.4; leftover $1284.80 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `QMCO` | 52 | $24.83 | $2.17 | $+3.49 | $1,312.42 | ▼ $9,640.25 (-359.75) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 65 | $19.57 | $2.21 | $-4.39 | $2,582.27 | ▼ $9,638.05 (-361.95) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ZENA` | 583 | $2.08 | $7.63 | $-82.19 | $3,790.19 | ▼ $9,630.42 (-369.58) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 115 | $9.57 | $2.36 | $-182.95 | $4,888.38 | ▼ $9,628.05 (-371.95) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LIFE` | 36 | $34.03 | $2.12 | $-40.58 | $6,111.34 | ▼ $9,625.94 (-374.06) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LUNR` | 67 | $20.25 | $2.21 | $+67.96 | $7,465.88 | ▼ $9,623.72 (-376.28) | dropped from list after 1 sess (min 1) | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 26 | $47.39 | $2.09 | $-41.34 | $8,695.93 | ▼ $9,621.64 (-378.36) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BZAI` | 1677 | $0.55 | $14.58 | $-391.33 | $9,607.06 | ▼ $9,607.06 (-392.94) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 286 | $4.19 | $3.69 | — | $8,405.03 | ▼ $9,603.37 (-396.63) | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ⚪; ret5=+291.8; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 174 | $6.87 | $2.51 | — | $7,207.14 | ▼ $9,600.86 (-399.14) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+62.6; leftover $1200.88 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `STDN` | 88 | $13.64 | $2.25 | — | $6,004.56 | ▼ $9,598.60 (-401.40) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+57.7; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,806.82 | ▼ $9,596.53 (-403.47) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ret5=+46.0; leftover $1200.88 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 36 | $32.55 | $2.10 | — | $3,632.92 | ▼ $9,594.43 (-405.57) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 81 | $14.66 | $2.23 | — | $2,443.23 | ▼ $9,592.20 (-407.80) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1200.88 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 625 | $1.92 | $8.06 | — | $1,235.16 | ▼ $9,584.13 (-415.87) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 80 | $14.94 | $2.23 | — | $37.73 | ▼ $9,581.90 (-418.10) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1200.88 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 286 | $3.94 | $3.75 | $-78.94 | $1,160.83 | ▼ $9,121.54 (-878.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `STDN` | 88 | $13.31 | $2.28 | $-33.57 | $2,329.83 | ▼ $9,119.26 (-880.74) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $3,531.23 | ▼ $9,117.16 (-882.84) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 36 | $28.59 | $2.12 | $-146.78 | $4,558.35 | ▼ $9,115.04 (-884.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 81 | $13.19 | $2.26 | $-123.56 | $5,624.49 | ▼ $9,112.79 (-887.21) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 625 | $1.70 | $8.18 | $-153.74 | $6,678.81 | ▼ $9,104.61 (-895.39) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 80 | $14.01 | $2.25 | $-78.88 | $7,797.36 | ▼ $9,102.36 (-897.64) | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **SELL** | `CAPR` | 174 | $7.19 | $2.55 | $+50.62 | $9,045.87 | ▼ $9,045.87 (-954.13) | dropped from list after 2 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRNA` | 7 | $150.14 | $2.01 | — | $7,992.88 | ▼ $9,043.86 (-956.14) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $1130.73 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CYPH` | 983 | $1.15 | $12.68 | — | $6,849.74 | ▼ $9,031.17 (-968.83) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABCL` | 95 | $11.81 | $2.27 | — | $5,725.04 | ▼ $9,028.90 (-971.10) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SENS` | 126 | $8.91 | $2.37 | — | $4,600.02 | ▼ $9,026.53 (-973.47) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+14.9; leftover $1130.73 | join🟡 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AUTL` | 457 | $2.47 | $5.90 | — | $3,465.33 | ▼ $9,020.64 (-979.36) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+19.8; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `TEM` | 18 | $61.83 | $2.04 | — | $2,350.35 | ▼ $9,018.59 (-981.41) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 7 | $144.54 | $2.01 | — | $1,336.56 | ▼ $9,016.58 (-983.42) | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 57 | $19.63 | $2.16 | — | $215.49 | ▼ $9,014.42 (-985.58) | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1130.73 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABCL` | 95 | $11.57 | $2.30 | $-27.85 | $1,312.33 | ▼ $9,305.08 (-694.92) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SENS` | 126 | $9.24 | $2.40 | $+36.81 | $2,474.18 | ▼ $9,302.69 (-697.31) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AUTL` | 457 | $2.47 | $5.98 | $-11.88 | $3,596.98 | ▼ $9,296.70 (-703.30) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 7 | $154.70 | $2.03 | $+67.08 | $4,677.85 | ▼ $9,294.67 (-705.33) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 57 | $21.17 | $2.18 | $+83.44 | $5,882.36 | ▼ $9,292.49 (-707.51) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `XHG` | 262 | $4.49 | $3.38 | — | $4,702.60 | ▼ $9,289.11 (-710.89) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.7; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 105 | $11.13 | $2.31 | — | $3,531.65 | ▼ $9,286.81 (-713.19) | rank by w_hot_cond; rank w_hot_cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `IOVA` | 129 | $9.08 | $2.38 | — | $2,357.95 | ▼ $9,284.43 (-715.57) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ⚪; ret5=+37.9; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CAPR` | 172 | $6.81 | $2.51 | — | $1,184.12 | ▼ $9,281.92 (-718.08) | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+62.5; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $107.24 | ▼ $9,279.91 (-720.09) | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1176.47 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `MRNA` | 7 | $142.70 | $2.03 | $-56.12 | $1,104.11 | ▲ $10,168.11 (+168.11) | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 983 | $1.83 | $12.86 | $+642.90 | $2,890.14 | ▲ $10,155.25 (+155.25) | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `TEM` | 18 | $70.07 | $2.06 | $+144.21 | $4,149.33 | ▲ $10,153.18 (+153.18) | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `XHG` | 262 | $4.24 | $3.43 | $-72.31 | $5,256.78 | ▲ $10,149.75 (+149.75) | dropped from list after 1 sess (min 1) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🔴 peer🟢 vol🔴 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 105 | $13.26 | $2.33 | $+219.01 | $6,646.75 | ▲ $10,147.42 (+147.42) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `IOVA` | 129 | $8.05 | $2.41 | $-137.66 | $7,682.79 | ▲ $10,145.01 (+145.01) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CAPR` | 172 | $8.01 | $2.55 | $+201.35 | $9,057.96 | ▲ $10,142.46 (+142.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 9 | $120.50 | $2.04 | $+5.58 | $10,140.43 | ▲ $10,140.43 (+140.43) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 745 | $1.70 | $9.61 | — | $8,864.32 | ▲ $10,130.82 (+130.82) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `XHG` | 315 | $4.02 | $4.06 | — | $7,593.95 | ▲ $10,126.75 (+126.75) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+16.1; leftover $1267.55 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $6,397.33 | ▲ $10,124.73 (+124.73) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 33 | $38.00 | $2.09 | — | $5,141.24 | ▲ $10,122.64 (+122.64) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 60 | $20.90 | $2.17 | — | $3,885.07 | ▲ $10,120.47 (+120.47) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer; 🔵; ret5=+47.9; leftover $1267.55 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 55 | $22.65 | $2.15 | — | $2,637.17 | ▲ $10,118.32 (+118.32) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; ⚪; ret5=+21.1; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 16 | $77.90 | $2.04 | — | $1,388.73 | ▲ $10,116.28 (+116.28) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `WPM` | 7 | $160.00 | $2.01 | — | $266.72 | ▲ $10,114.27 (+114.27) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot,mover_buy; ⚪; ret5=+17.6; leftover $1267.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 745 | $1.60 | $9.74 | $-93.85 | $1,448.98 | ▲ $10,054.56 (+54.56) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `XHG` | 315 | $3.81 | $4.13 | $-74.34 | $2,645.00 | ▲ $10,050.43 (+50.43) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $3,840.96 | ▲ $10,048.39 (+48.39) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ERO` | 33 | $40.51 | $2.11 | $+78.63 | $5,175.68 | ▲ $10,046.28 (+46.28) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 60 | $20.72 | $2.19 | $-15.16 | $6,416.69 | ▲ $10,044.09 (+44.09) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HMY` | 55 | $22.39 | $2.17 | $-18.63 | $7,645.96 | ▲ $10,041.91 (+41.91) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FCX` | 16 | $79.34 | $2.06 | $+18.94 | $8,913.35 | ▲ $10,039.86 (+39.86) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 7 | $160.93 | $2.03 | $+2.47 | $10,037.83 | ▲ $10,037.83 (+37.83) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 50 | $24.84 | $2.14 | — | $8,793.69 | ▲ $10,035.69 (+35.69) | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+13.0; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 484 | $2.59 | $6.24 | — | $7,533.88 | ▲ $10,029.44 (+29.44) | rank by w_hot_cond; rank w_hot_cond; list flatten; ret5=+4.2; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 80 | $15.60 | $2.23 | — | $6,283.65 | ▲ $10,027.21 (+27.21) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+7.1; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 22 | $55.20 | $2.06 | — | $5,067.20 | ▲ $10,025.16 (+25.16) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.0; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MRVL` | 5 | $240.00 | $2.00 | — | $3,865.19 | ▲ $10,023.15 (+23.15) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+6.8; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $2,937.46 | ▲ $10,021.16 (+21.16) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=-0.5; leftover $1254.73 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 7 | $170.60 | $2.01 | — | $1,741.25 | ▲ $10,019.15 (+19.15) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=+3.4; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 16 | $75.12 | $2.04 | — | $537.29 | ▲ $10,017.11 (+17.11) | rank by w_hot_cond; rank w_hot_cond; list mover_buy; 🔵; ret5=-2.2; leftover $1254.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 50 | $24.00 | $2.16 | $-46.30 | $1,735.13 | ▲ $10,113.53 (+113.53) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 484 | $2.60 | $6.33 | $-7.74 | $2,987.19 | ▲ $10,107.19 (+107.19) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 80 | $15.33 | $2.25 | $-26.08 | $4,211.34 | ▲ $10,104.94 (+104.94) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 22 | $55.25 | $2.08 | $-3.03 | $5,424.77 | ▲ $10,102.87 (+102.87) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVL` | 5 | $253.44 | $2.03 | $+63.17 | $6,689.94 | ▲ $10,100.84 (+100.84) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $7,654.94 | ▲ $10,098.83 (+98.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 7 | $178.75 | $2.03 | $+53.01 | $8,904.16 | ▲ $10,096.80 (+96.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 16 | $74.54 | $2.06 | $-13.38 | $10,094.74 | ▲ $10,094.74 (+94.74) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ERO` | 32 | $39.20 | $2.09 | — | $8,838.25 | ▲ $10,092.65 (+92.65) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.6; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `FIGR` | 33 | $37.42 | $2.09 | — | $7,601.30 | ▲ $10,090.56 (+90.56) | rank by w_hot_cond; rank w_hot_cond; list yday_mover; ret5=+24.4; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `BKKT` | 148 | $8.50 | $2.43 | — | $6,340.87 | ▲ $10,088.13 (+88.13) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+12.3; leftover $1261.84 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `FCX` | 16 | $78.83 | $2.04 | — | $5,077.55 | ▲ $10,086.09 (+86.09) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.3; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `QMCO` | 53 | $23.50 | $2.15 | — | $3,829.90 | ▲ $10,083.94 (+83.94) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=-14.8; leftover $1261.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 229 | $5.49 | $2.95 | — | $2,569.74 | ▲ $10,080.99 (+80.99) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+15.9; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `NIQ` | 67 | $18.79 | $2.19 | — | $1,308.62 | ▲ $10,078.80 (+78.80) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+7.6; leftover $1261.84 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VIRT` | 19 | $65.42 | $2.05 | — | $63.59 | ▲ $10,076.75 (+76.75) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+13.2; leftover $1261.84 | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ERO` | 32 | $38.60 | $2.11 | $-23.39 | $1,296.68 | ▼ $9,641.37 (-358.63) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-31 09:30 ET | **SELL** | `FIGR` | 33 | $35.50 | $2.11 | $-67.56 | $2,466.08 | ▼ $9,639.27 (-360.73) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BKKT` | 148 | $7.58 | $2.47 | $-141.06 | $3,585.45 | ▼ $9,636.80 (-363.20) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `FCX` | 16 | $76.10 | $2.06 | $-47.78 | $4,800.99 | ▼ $9,634.74 (-365.26) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `QMCO` | 53 | $21.70 | $2.17 | $-99.72 | $5,948.92 | ▼ $9,632.57 (-367.43) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `NIQ` | 67 | $19.20 | $2.21 | $+23.07 | $7,233.11 | ▼ $9,630.36 (-369.64) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🔴 digest🟢 judge🟡 ab🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `VIRT` | 19 | $66.39 | $2.07 | $+14.32 | $8,492.45 | ▼ $9,628.29 (-371.71) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `TIGR` | 229 | $5.02 | $3.00 | $-113.59 | $9,639.03 | ▼ $9,639.03 (-360.97) | dropped from list after 2 sess (min 1) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $8,577.22 | ▼ $9,637.02 (-362.98) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 73 | $16.46 | $2.21 | — | $7,373.43 | ▼ $9,634.81 (-365.19) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `XHG` | 337 | $3.57 | $4.35 | — | $6,165.99 | ▼ $9,630.46 (-369.54) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.1; leftover $1204.88 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CAN` | 4016 | $0.30 | $24.10 | — | $4,937.09 | ▼ $9,606.36 (-393.64) | rank by w_hot_cond; rank w_hot_cond; list yday_mover; 🔵; ret5=+54.3; leftover $1204.88 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 117 | $10.27 | $2.34 | — | $3,733.16 | ▼ $9,604.02 (-395.98) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `INO` | 899 | $1.34 | $11.60 | — | $2,516.91 | ▼ $9,592.43 (-407.57) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $1,381.43 | ▼ $9,590.41 (-409.59) | rank by w_hot_cond; rank w_hot_cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ZYME` | 40 | $30.00 | $2.11 | — | $179.32 | ▼ $9,588.30 (-411.70) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+14.1; leftover $1204.88 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟡 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 7 | $145.95 | $2.03 | $-42.19 | $1,198.94 | ▼ $9,822.90 (-177.10) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 73 | $16.77 | $2.23 | $+18.19 | $2,420.92 | ▼ $9,820.67 (-179.33) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAN` | 4016 | $0.34 | $26.38 | $+110.16 | $3,759.98 | ▼ $9,794.29 (-205.71) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 117 | $10.41 | $2.37 | $+11.67 | $4,975.58 | ▼ $9,791.92 (-208.08) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $6,165.59 | ▼ $9,789.88 (-210.12) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 242 | $5.08 | $3.12 | — | $4,933.11 | ▼ $9,786.76 (-213.24) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TRLV` | 103 | $11.89 | $2.30 | — | $3,706.14 | ▼ $9,784.46 (-215.54) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 456 | $2.70 | $5.88 | — | $2,469.06 | ▼ $9,778.58 (-221.42) | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OMER` | 64 | $18.99 | $2.18 | — | $1,251.51 | ▼ $9,776.39 (-223.61) | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.1; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $33.22 | ▼ $9,774.34 (-225.66) | rank by w_hot_cond; rank w_hot_cond; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1233.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AVAH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `IOVA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ARX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SENS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `REAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HMY` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-26 | `AEM` | no_price | no 09:30 open |
| 2026-08-26 | `SCCO` | no_price | no 09:30 open |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `XHG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NVAX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `VFF` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OBE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `INO` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `XHG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TXG` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZYME` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DK` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `OMER` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `NVAX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ALVO` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 337 | 2026-09-03 @ $3.57 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ret5=+16.1; leftover $1204.88 |
| `INO` | 899 | 2026-09-03 @ $1.34 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+8.3; leftover $1204.88 |
| `ZYME` | 40 | 2026-09-03 @ $30.00 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; ⚪; ret5=+14.1; leftover $1204.88 |
| `OABI` | 242 | 2026-09-04 @ $5.08 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1233.12 |
| `TRLV` | 103 | 2026-09-04 @ $11.89 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ⚪; ret5=+15.0; leftover $1233.12 |
| `ALEC` | 456 | 2026-09-04 @ $2.70 | rank by w_hot_cond; rank w_hot_cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1233.12 |
| `OMER` | 64 | 2026-09-04 @ $18.99 | rank by w_hot_cond; rank w_hot_cond; list ohlc_hot; 🔵; ret5=+12.1; leftover $1233.12 |
| `ATRC` | 23 | 2026-09-04 @ $52.88 | rank by w_hot_cond; rank w_hot_cond; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1233.12 |
