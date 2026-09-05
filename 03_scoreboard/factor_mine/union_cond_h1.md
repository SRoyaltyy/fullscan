# Factor mine action — `union_cond_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `cond` · size `leftover` · sell `list` · S-boost `none` · rank by cond

Cash book **-2.31%** ($9,769) · signal-only (no cash/fees) was -2.71%. Starts YES **3/17**. Fills 148 · skips 58 · realized $+151.72.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `cond` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $80.42.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, HIMS, INO, IREN, SLS, TGTX, TNDM, TPG | — | $97.53 | $10,055.59 | $10,153.12 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24 | BUY BTSG x20 @ 59.80; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY IREN x27 @ 45.98; BUY SLS x106 @ 11.70; BUY TGTX x25 @ 49.70; BUY TNDM x53 @ 23.33; BUY TPG x24 @ 50.62 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24 | ARX, BETR, BTBT, FIGR, ADUR, AIRO, AMPY, ANGX | BTSG, HIMS, INO, IREN, SLS, TGTX, TNDM, TPG | $30.71 | $9,800.95 | $9,831.66 | ARX×64, BETR×85, BTBT×845, FIGR×39, ADUR×76, AIRO×114, AMPY×256, ANGX×294 | SELL BTSG (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); BUY ARX x64 @ 19.57; BUY BETR x85 @ 14.80; BUY BTBT x845 @ 1.50; BUY FIGR x39 @ 32.12; BUY ADUR x76 @ 16.50; BUY AIRO x114 @ 11.12; BUY AMPY x256 @ 4.94; BUY ANGX x294 @ 4.31 |
| 2026-08-17 | +2.25 | $30.71 | ARX×64, BETR×85, BTBT×845, FIGR×39, ADUR×76, AIRO×114, AMPY×256, ANGX×294 | ABX, INV, NU, XHG, DVN, EOG, FANG, ALOY | ARX, BETR, BTBT, FIGR, ADUR, AIRO, AMPY, ANGX | $139.07 | $9,377.11 | $9,516.18 | ABX×134, INV×759, NU×79, XHG×293, DVN×26, EOG×8, FANG×6, ALOY×83 | SELL ARX (dropped from list after 1 sess (min 1)); SELL BETR (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL FIGR (dropped from list after 1 sess (min 1)); SELL ADUR (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL AMPY (dropped from list after 1 sess (min 1)); SELL ANGX (dropped from list after 1 sess (min 1)); BUY ABX x134 @ 9.12; BUY INV x759 @ 1.62; BUY NU x79 @ 15.40; BUY XHG x293 @ 4.19; BUY DVN x26 @ 46.18; BUY EOG x8 @ 142.77; BUY FANG x6 @ 202.70; BUY ALOY x83 @ 14.66 |
| 2026-08-18 | -6.20 | $139.07 | ABX×134, INV×759, NU×79, XHG×293, DVN×26, EOG×8, FANG×6, ALOY×83 | — | ABX, INV, NU, XHG, DVN, EOG, FANG, ALOY | $9,410.87 | $0.00 | $9,410.87 | — | SELL ABX (dropped from list after 1 sess (min 1)); SELL INV (dropped from list after 1 sess (min 1)); SELL NU (dropped from list after 1 sess (min 1)); SELL XHG (dropped from list after 1 sess (min 1)); SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL ALOY (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,410.87 | — | — | — | $9,410.87 | $0.00 | $9,410.87 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,410.87 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $149.71 | $9,457.53 | $9,607.24 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8 | BUY AG x57 @ 20.55; BUY BHP x12 @ 91.01; BUY CDE x56 @ 20.65; BUY HDSN x203 @ 5.77; BUY IAG x59 @ 19.63; BUY KGC x39 @ 29.63; BUY NFGC x672 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $149.71 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $196.74 | $9,848.76 | $10,045.50 | AU×10, AUPH×71, AEM×5, ARCT×110, AUTL×497, CRDL×637, CRSP×20, CYPH×931 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x10 @ 119.43; BUY AUPH x71 @ 17.20; BUY AEM x5 @ 216.30; BUY ARCT x110 @ 11.13; BUY AUTL x497 @ 2.47; BUY CRDL x637 @ 1.93; BUY CRSP x20 @ 59.72; BUY CYPH x931 @ 1.32 |
| 2026-08-24 | -5.17 | $196.74 | AU×10, AUPH×71, AEM×5, ARCT×110, AUTL×497, CRDL×637, CRSP×20, CYPH×931 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CYPH | $9,156.28 | $1,138.20 | $10,294.48 | CRSP×20 | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $9,156.28 | CRSP×20 | AU, ERO, FCX, CNH, HMY, MOS, RHI, SUZ | CRSP | $238.64 | $10,042.26 | $10,280.90 | AU×10, ERO×33, FCX×16, CNH×109, HMY×56, MOS×53, RHI×28, SUZ×141 | SELL CRSP (dropped from list after 2 sess (min 1)); BUY AU x10 @ 119.46; BUY ERO x33 @ 38.00; BUY FCX x16 @ 77.90; BUY CNH x109 @ 11.72; BUY HMY x56 @ 22.65; BUY MOS x53 @ 24.00; BUY RHI x28 @ 44.52; BUY SUZ x141 @ 9.07 |
| 2026-08-26 | +2.02 | $238.64 | AU×10, ERO×33, FCX×16, CNH×109, HMY×56, MOS×53, RHI×28, SUZ×141 | — | — | $238.64 | $10,038.31 | $10,276.95 | AU×10, ERO×33, FCX×16, CNH×109, HMY×56, MOS×53, RHI×28, SUZ×141 | hold AU,ERO,FCX,CNH,HMY,MOS,RHI,SUZ |
| 2026-08-27 | — | $238.64 | AU×10, ERO×33, FCX×16, CNH×109, HMY×56, MOS×53, RHI×28, SUZ×141 | ACMR, GGB, MT, MU, SLI, TX, ANET | AU, ERO, FCX, CNH, HMY, RHI, SUZ | $618.09 | $9,758.74 | $10,376.83 | MOS×53, ACMR×15, GGB×292, MT×17, MU×1, SLI×499, TX×23, ANET×6 | SELL AU (dropped from list after 2 sess (min 1)); SELL ERO (dropped from list after 2 sess (min 1)); SELL FCX (dropped from list after 2 sess (min 1)); SELL CNH (dropped from list after 2 sess (min 1)); SELL HMY (dropped from list after 2 sess (min 1)); SELL RHI (dropped from list after 2 sess (min 1)); SELL SUZ (dropped from list after 2 sess (min 1)); BUY ACMR x15 @ 80.97; BUY GGB x292 @ 4.42; BUY MT x17 @ 75.12; BUY MU x1 @ 925.74; BUY SLI x499 @ 2.59; BUY TX x23 @ 55.20; BUY ANET x6 @ 190.90 |
| 2026-08-28 | +0.75 | $618.09 | MOS×53, ACMR×15, GGB×292, MT×17, MU×1, SLI×499, TX×23, ANET×6 | KEYS, SMTC, CIEN, AVT, CGNX, COHR, LSCC | MOS, ACMR, GGB, MT, MU, SLI, TX, ANET | $1,767.55 | $8,569.07 | $10,336.62 | KEYS×4, SMTC×8, CIEN×3, AVT×14, CGNX×20, COHR×4, LSCC×10 | SELL MOS (dropped from list after 3 sess (min 1)); SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL TX (dropped from list after 1 sess (min 1)); SELL ANET (dropped from list after 1 sess (min 1)); BUY KEYS x4 @ 323.82; BUY SMTC x8 @ 149.40; BUY CIEN x3 @ 411.53; BUY AVT x14 @ 91.11; BUY CGNX x20 @ 62.80; BUY COHR x4 @ 303.67; BUY LSCC x10 @ 121.13 |
| 2026-08-31 | -5.85 | $1,767.55 | KEYS×4, SMTC×8, CIEN×3, AVT×14, CGNX×20, COHR×4, LSCC×10 | — | KEYS, SMTC, CIEN, AVT, CGNX, COHR, LSCC | $9,938.75 | $0.00 | $9,938.75 | — | SELL KEYS (dropped from list after 1 sess (min 1)); SELL SMTC (dropped from list after 1 sess (min 1)); SELL CIEN (dropped from list after 1 sess (min 1)); SELL AVT (dropped from list after 1 sess (min 1)); SELL CGNX (dropped from list after 1 sess (min 1)); SELL COHR (dropped from list after 1 sess (min 1)); SELL LSCC (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $9,938.75 | — | — | — | $9,938.75 | $0.00 | $9,938.75 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $9,938.75 | — | — | — | $9,938.75 | $0.00 | $9,938.75 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,938.75 | — | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | — | $149.76 | $9,975.38 | $10,125.14 | ARCT×75, BMEA×690, CRDL×575, HRMY×30, NVAX×120, PBH×23, PCRX×46, RVTY×9 | BUY ARCT x75 @ 16.46; BUY BMEA x690 @ 1.80; BUY CRDL x575 @ 2.16; BUY HRMY x30 @ 41.31; BUY NVAX x120 @ 10.27; BUY PBH x23 @ 52.88; BUY PCRX x46 @ 26.52; BUY RVTY x9 @ 125.94 |
| 2026-09-04 | — | $149.76 | ARCT×75, BMEA×690, CRDL×575, HRMY×30, NVAX×120, PBH×23, PCRX×46, RVTY×9 | CABA, ALEC, ATRC, MLYS, OABI, OPK, TARS, BVS | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | $80.42 | $9,688.74 | $9,769.16 | CABA×349, ALEC×469, ATRC×23, MLYS×43, OABI×249, OPK×742, TARS×15, BVS×87 | SELL ARCT (dropped from list after 1 sess (min 1)); SELL BMEA (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL HRMY (dropped from list after 1 sess (min 1)); SELL NVAX (dropped from list after 1 sess (min 1)); SELL PBH (dropped from list after 1 sess (min 1)); SELL PCRX (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); BUY CABA x349 @ 3.63; BUY ALEC x469 @ 2.70; BUY ATRC x23 @ 52.88; BUY MLYS x43 @ 29.15; BUY OABI x249 @ 5.08; BUY OPK x742 @ 1.71; BUY TARS x15 @ 82.76; BUY BVS x87 @ 14.50 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $7,550.75 | rank by cond; rank cond; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $6,283.80 | rank by cond; rank cond; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $5,040.27 | rank by cond; rank cond; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,797.76 | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $2,553.19 | rank by cond; rank cond; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $1,314.55 | rank by cond; rank cond; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $97.53 | rank by cond; rank cond; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $2,510.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $3,926.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $5,114.71 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,426.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $7,606.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $8,819.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $10,143.91 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $8,889.25 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 85 | $14.80 | $2.25 | — | $7,629.00 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $6,350.60 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FIGR` | 39 | $32.12 | $2.11 | — | $5,095.81 | rank by cond; rank cond; list ohlc_hot; 🔵; ⚪; ret5=+11.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $3,839.60 | rank by cond; rank cond; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 114 | $11.12 | $2.33 | — | $2,569.58 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 256 | $4.94 | $3.30 | — | $1,301.64 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 294 | $4.31 | $3.79 | — | $30.71 | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $1,280.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 85 | $13.67 | $2.27 | $-100.56 | $2,440.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $3,714.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `FIGR` | 39 | $32.16 | $2.13 | $-2.67 | $4,966.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $6,159.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 114 | $9.57 | $2.36 | $-181.39 | $7,247.99 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AMPY` | 256 | $4.86 | $3.35 | $-27.14 | $8,488.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 294 | $4.60 | $3.85 | $+77.62 | $9,837.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 134 | $9.12 | $2.39 | — | $8,612.87 | rank by cond; rank cond; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 759 | $1.62 | $9.79 | — | $7,373.50 | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NU` | 79 | $15.40 | $2.23 | — | $6,154.67 | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+10.0; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 293 | $4.19 | $3.78 | — | $4,923.22 | rank by cond; rank cond; list yday_mover; ⚪; ret5=+291.8; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $3,720.47 | rank by cond; rank cond; list flatten; 🔵; ret5=+6.7; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $2,576.30 | rank by cond; rank cond; list flatten; 🔵; ret5=+5.8; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $1,358.09 | rank by cond; rank cond; list flatten; 🔵; ret5=+8.3; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 83 | $14.66 | $2.24 | — | $139.07 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1229.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 134 | $9.03 | $2.42 | $-16.88 | $1,346.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `INV` | 759 | $1.32 | $9.93 | $-243.62 | $2,342.42 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `NU` | 79 | $14.53 | $2.25 | $-73.21 | $3,488.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `XHG` | 293 | $3.94 | $3.84 | $-80.87 | $4,638.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $5,884.53 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $7,066.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $8,318.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 83 | $13.19 | $2.26 | $-126.51 | $9,410.87 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,237.36 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,143.22 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $5,984.66 | rank by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 203 | $5.77 | $2.62 | — | $4,810.73 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $3,650.39 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,492.72 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 672 | $1.75 | $8.67 | — | $1,308.05 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $149.71 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1176.36 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 57 | $21.90 | $2.18 | $+72.61 | $1,395.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 12 | $95.72 | $2.05 | $+52.45 | $2,542.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $3,758.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 203 | $5.67 | $2.66 | $-25.58 | $4,906.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $6,153.44 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $7,405.94 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 672 | $1.79 | $8.79 | $+9.42 | $8,600.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,835.60 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,639.28 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 71 | $17.20 | $2.20 | — | $7,415.88 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,332.37 | rank by cond; rank cond; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 110 | $11.13 | $2.32 | — | $5,105.75 | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 497 | $2.47 | $6.41 | — | $3,871.75 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 637 | $1.93 | $8.22 | — | $2,634.12 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 20 | $59.72 | $2.05 | — | $1,437.67 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 931 | $1.32 | $12.01 | — | $196.74 | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1229.45 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,399.70 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 71 | $16.60 | $2.22 | $-47.03 | $2,576.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,659.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 110 | $13.26 | $2.35 | $+229.63 | $5,115.45 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 497 | $2.36 | $6.50 | $-67.59 | $6,281.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 637 | $1.87 | $8.33 | $-54.77 | $7,464.73 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 931 | $1.83 | $12.18 | $+450.62 | $9,156.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **SELL** | `CRSP` | 20 | $57.00 | $2.07 | $-58.52 | $10,294.21 | dropped from list after 2 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 10 | $119.46 | $2.02 | — | $9,097.59 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1286.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 33 | $38.00 | $2.09 | — | $7,841.50 | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1286.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 16 | $77.90 | $2.04 | — | $6,593.06 | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1286.78 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 109 | $11.72 | $2.32 | — | $5,313.26 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1286.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 56 | $22.65 | $2.16 | — | $4,042.71 | rank by cond; rank cond; list mover_buy; ⚪; ret5=+21.1; leftover $1286.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 53 | $24.00 | $2.15 | — | $2,768.56 | rank by cond; rank cond; list flatten; ⚪; ret5=+13.0; leftover $1286.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 28 | $44.52 | $2.07 | — | $1,519.92 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+3.5; leftover $1286.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 141 | $9.07 | $2.41 | — | $238.64 | rank by cond; rank cond; list mover_buy; ⚪; ret5=+8.3; leftover $1286.78 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AU` | 10 | $119.80 | $2.04 | $-0.66 | $1,434.60 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ERO` | 33 | $40.51 | $2.11 | $+78.63 | $2,769.32 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `FCX` | 16 | $79.34 | $2.06 | $+18.94 | $4,036.70 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CNH` | 109 | $11.54 | $2.35 | $-24.28 | $5,292.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HMY` | 56 | $22.39 | $2.18 | $-18.90 | $6,543.88 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RHI` | 28 | $44.33 | $2.09 | $-9.49 | $7,783.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SUZ` | 141 | $9.03 | $2.45 | $-10.50 | $9,053.81 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 15 | $80.97 | $2.04 | — | $7,837.22 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-1.3; leftover $1293.40 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 292 | $4.42 | $3.77 | — | $6,542.82 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-8.6; leftover $1293.40 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 17 | $75.12 | $2.04 | — | $5,263.73 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-2.2; leftover $1293.40 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $4,336.00 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-0.5; leftover $1293.40 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 499 | $2.59 | $6.44 | — | $3,037.15 | rank by cond; rank cond; list flatten; ret5=+4.2; leftover $1293.40 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 23 | $55.20 | $2.06 | — | $1,765.50 | rank by cond; rank cond; list mover_buy; 🔵; ret5=+3.0; leftover $1293.40 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 6 | $190.90 | $2.01 | — | $618.09 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-5.1; leftover $1293.40 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 53 | $24.00 | $2.17 | $-4.32 | $1,887.92 | dropped from list after 3 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 15 | $81.65 | $2.06 | $+6.11 | $3,110.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 292 | $4.57 | $3.83 | $+36.21 | $4,441.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 17 | $74.54 | $2.06 | $-13.96 | $5,706.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $6,671.34 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 499 | $2.60 | $6.53 | $-7.98 | $7,962.21 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 23 | $55.25 | $2.08 | $-2.99 | $9,230.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 6 | $205.90 | $2.03 | $+85.96 | $10,464.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $9,166.97 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1308.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $7,969.76 | rank by cond; rank cond; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1308.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $411.53 | $2.00 | — | $6,733.17 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-7.7; leftover $1308.03 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $5,455.60 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1308.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 20 | $62.80 | $2.05 | — | $4,197.55 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1308.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $2,980.87 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1308.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $121.13 | $2.02 | — | $1,767.55 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1308.03 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `KEYS` | 4 | $324.14 | $2.02 | $-2.74 | $3,062.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 8 | $133.04 | $2.03 | $-134.93 | $4,124.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CIEN` | 3 | $373.68 | $2.02 | $-117.57 | $5,243.39 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `AVT` | 14 | $88.63 | $2.05 | $-38.80 | $6,482.16 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CGNX` | 20 | $60.31 | $2.07 | $-53.92 | $7,686.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `COHR` | 4 | $274.13 | $2.02 | $-122.18 | $8,780.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LSCC` | 10 | $116.00 | $2.04 | $-55.36 | $9,938.75 | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.46 | $2.21 | — | $8,702.03 | rank by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1242.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 690 | $1.80 | $8.90 | — | $7,451.13 | rank by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1242.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 575 | $2.16 | $7.42 | — | $6,201.71 | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1242.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $4,960.33 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1242.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 120 | $10.27 | $2.35 | — | $3,725.58 | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1242.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $52.88 | $2.06 | — | $2,507.29 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-0.1; leftover $1242.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 46 | $26.52 | $2.13 | — | $1,285.24 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1242.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $149.76 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1242.34 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 75 | $16.77 | $2.24 | $+18.80 | $1,405.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `BMEA` | 690 | $1.93 | $9.03 | $+71.77 | $2,727.95 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 575 | $2.18 | $7.52 | $-3.44 | $3,973.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 30 | $42.93 | $2.10 | $+44.42 | $5,259.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 120 | $10.41 | $2.38 | $+12.07 | $6,506.54 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `PBH` | 23 | $53.45 | $2.08 | $+8.97 | $7,733.81 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `PCRX` | 46 | $26.74 | $2.15 | $+5.84 | $8,961.71 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $10,151.72 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 349 | $3.63 | $4.50 | — | $8,880.35 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1268.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 469 | $2.70 | $6.05 | — | $7,608.00 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1268.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ATRC` | 23 | $52.88 | $2.06 | — | $6,389.70 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1268.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $29.15 | $2.12 | — | $5,134.13 | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1268.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 249 | $5.08 | $3.21 | — | $3,866.00 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1268.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 742 | $1.71 | $9.57 | — | $2,587.61 | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1268.96 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 15 | $82.76 | $2.04 | — | $1,344.17 | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1268.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 87 | $14.50 | $2.25 | — | $80.42 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1268.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AURA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OCUL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `RLX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ZLAB` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BHP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BSBR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `EBAY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NOK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TME` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALM` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GFI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `KGC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CNH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HMY` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RHI` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SUZ` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AEM` | no_price | no 09:30 open |
| 2026-08-26 | `HOOD` | no_price | no 09:30 open |
| 2026-08-26 | `SCCO` | no_price | no 09:30 open |
| 2026-08-26 | `WPM` | no_price | no 09:30 open |
| 2026-08-26 | `SSRM` | no_price | no 09:30 open |
| 2026-08-28 | `MPWR` | cash | leftover split 1308.03 < 1 share @ 1319.75 |
| 2026-08-31 | `MPC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACIW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `AVPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CDNS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CHKP` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CVI` | hard_red | hard-red S=-5.85 sit; no new buys |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `CABA` | 349 | 2026-09-04 @ $3.63 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1268.96 |
| `ALEC` | 469 | 2026-09-04 @ $2.70 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1268.96 |
| `ATRC` | 23 | 2026-09-04 @ $52.88 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1268.96 |
| `MLYS` | 43 | 2026-09-04 @ $29.15 | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1268.96 |
| `OABI` | 249 | 2026-09-04 @ $5.08 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1268.96 |
| `OPK` | 742 | 2026-09-04 @ $1.71 | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $1268.96 |
| `TARS` | 15 | 2026-09-04 @ $82.76 | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1268.96 |
| `BVS` | 87 | 2026-09-04 @ $14.50 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1268.96 |
