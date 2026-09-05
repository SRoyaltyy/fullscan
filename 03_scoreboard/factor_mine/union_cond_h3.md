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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, HIMS, INO, IREN, SLS, TGTX, TNDM, TPG | — | $97.53 | $10,055.59 | $10,153.12 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24 | BUY BTSG x20 @ 59.80; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY IREN x27 @ 45.98; BUY SLS x106 @ 11.70; BUY TGTX x25 @ 49.70; BUY TNDM x53 @ 23.33; BUY TPG x24 @ 50.62 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24 | BTBT, AIRO, AMPY, ANGX | — | $55.46 | $10,378.48 | $10,433.94 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24, BTBT×8, AIRO×1, AMPY×2, ANGX×2 | BUY BTBT x8 @ 1.50; BUY AIRO x1 @ 11.12; BUY AMPY x2 @ 4.94; BUY ANGX x2 @ 4.31 |
| 2026-08-17 | +2.25 | $55.46 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24, BTBT×8, AIRO×1, AMPY×2, ANGX×2 | INV, XHG | — | $44.67 | $10,478.68 | $10,523.35 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24, BTBT×8, AIRO×1, AMPY×2, ANGX×2, INV×4, XHG×1 | BUY INV x4 @ 1.62; BUY XHG x1 @ 4.19 |
| 2026-08-18 | -6.20 | $44.67 | BTSG×20, HIMS×42, INO×1543, IREN×27, SLS×106, TGTX×25, TNDM×53, TPG×24, BTBT×8, AIRO×1, AMPY×2, ANGX×2, INV×4, XHG×1 | — | BTSG, HIMS, INO, IREN, SLS, TGTX, TNDM, TPG | $10,305.28 | $49.50 | $10,354.78 | BTBT×8, AIRO×1, AMPY×2, ANGX×2, INV×4, XHG×1 | SELL BTSG (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL TGTX (dropped from list after 3 sess (min 3)); SELL TNDM (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,305.28 | BTBT×8, AIRO×1, AMPY×2, ANGX×2, INV×4, XHG×1 | — | BTBT, AIRO, AMPY, ANGX | $10,344.57 | $10.49 | $10,355.06 | INV×4, XHG×1 | SELL BTBT (dropped from list after 3 sess (min 3)); SELL AIRO (dropped from list after 3 sess (min 3)); SELL AMPY (dropped from list after 3 sess (min 3)); SELL ANGX (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,344.57 | INV×4, XHG×1 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | INV, XHG | $208.95 | $10,360.34 | $10,569.29 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | SELL INV (dropped from list after 3 sess (min 3)); SELL XHG (dropped from list after 3 sess (min 3)); BUY AG x62 @ 20.55; BUY BHP x14 @ 91.01; BUY CDE x62 @ 20.65; BUY HDSN x224 @ 5.77; BUY IAG x65 @ 19.63; BUY KGC x43 @ 29.63; BUY NFGC x739 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $208.95 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $93.35 | $10,750.85 | $10,844.20 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | BUY AUPH x1 @ 17.20; BUY ARCT x2 @ 11.13; BUY AUTL x10 @ 2.47; BUY CRDL x13 @ 1.93; BUY CYPH x19 @ 1.32 |
| 2026-08-24 | -5.17 | $93.35 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | — | — | $93.35 | $10,721.83 | $10,815.18 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $93.35 | AG×62, BHP×14, CDE×62, HDSN×224, IAG×65, KGC×43, NFGC×739, WPM×8, AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19 | AU, ERO, FCX, CNH, HMY, MOS, RHI, SUZ | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $87.01 | $10,757.83 | $10,844.84 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); BUY AU x11 @ 119.46; BUY ERO x35 @ 38.00; BUY FCX x17 @ 77.90; BUY CNH x114 @ 11.72; BUY HMY x59 @ 22.65; BUY MOS x55 @ 24.00; BUY RHI x30 @ 44.52; BUY SUZ x147 @ 9.07 |
| 2026-08-26 | +2.02 | $87.01 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147 | — | — | $87.01 | $10,755.27 | $10,842.28 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147 | hold AUPH,ARCT,AUTL,CRDL,CYPH,AU,ERO,FCX,CNH,HMY,MOS,RHI,SUZ |
| 2026-08-27 | — | $87.01 | AUPH×1, ARCT×2, AUTL×10, CRDL×13, CYPH×19, AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147 | GGB, SLI | AUPH, ARCT, AUTL, CRDL, CYPH | $158.07 | $10,698.31 | $10,856.38 | AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147, GGB×6, SLI×11 | SELL AUPH (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); BUY GGB x6 @ 4.42; BUY SLI x11 @ 2.59 |
| 2026-08-28 | +0.75 | $158.07 | AU×11, ERO×35, FCX×17, CNH×114, HMY×59, MOS×55, RHI×30, SUZ×147, GGB×6, SLI×11 | KEYS, SMTC, CIEN, MPWR, AVT, CGNX, COHR, LSCC | AU, ERO, FCX, CNH, HMY, MOS, RHI, SUZ | $566.57 | $10,000.36 | $10,566.93 | GGB×6, SLI×11, KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | SELL AU (dropped from list after 3 sess (min 3)); SELL ERO (dropped from list after 3 sess (min 3)); SELL FCX (dropped from list after 3 sess (min 3)); SELL CNH (dropped from list after 3 sess (min 3)); SELL HMY (dropped from list after 3 sess (min 3)); SELL MOS (dropped from list after 3 sess (min 3)); SELL RHI (dropped from list after 3 sess (min 3)); SELL SUZ (dropped from list after 3 sess (min 3)); BUY KEYS x4 @ 323.82; BUY SMTC x8 @ 149.40; BUY CIEN x3 @ 411.53; BUY MPWR x1 @ 1319.75; BUY AVT x14 @ 91.11; BUY CGNX x21 @ 62.80; BUY COHR x4 @ 303.67; BUY LSCC x10 @ 121.13 |
| 2026-08-31 | -5.85 | $566.57 | GGB×6, SLI×11, KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | — | — | $566.57 | $9,579.69 | $10,146.26 | GGB×6, SLI×11, KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $566.57 | GGB×6, SLI×11, KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | — | GGB, SLI | $623.26 | $9,473.25 | $10,096.51 | KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | SELL GGB (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $623.26 | KEYS×4, SMTC×8, CIEN×3, MPWR×1, AVT×14, CGNX×21, COHR×4, LSCC×10 | — | KEYS, SMTC, CIEN, MPWR, AVT, CGNX, COHR, LSCC | $10,001.93 | $0.00 | $10,001.93 | — | SELL KEYS (dropped from list after 3 sess (min 3)); SELL SMTC (dropped from list after 3 sess (min 3)); SELL CIEN (dropped from list after 3 sess (min 3)); SELL MPWR (dropped from list after 3 sess (min 3)); SELL AVT (dropped from list after 3 sess (min 3)); SELL CGNX (dropped from list after 3 sess (min 3)); SELL COHR (dropped from list after 3 sess (min 3)); SELL LSCC (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,001.93 | — | ARCT, BMEA, CRDL, HRMY, NVAX, PBH, PCRX, RVTY | — | $162.37 | $10,026.51 | $10,188.88 | ARCT×75, BMEA×694, CRDL×578, HRMY×30, NVAX×121, PBH×23, PCRX×47, RVTY×9 | BUY ARCT x75 @ 16.46; BUY BMEA x694 @ 1.80; BUY CRDL x578 @ 2.16; BUY HRMY x30 @ 41.31; BUY NVAX x121 @ 10.27; BUY PBH x23 @ 52.88; BUY PCRX x47 @ 26.52; BUY RVTY x9 @ 125.94 |
| 2026-09-04 | — | $162.37 | ARCT×75, BMEA×694, CRDL×578, HRMY×30, NVAX×121, PBH×23, PCRX×47, RVTY×9 | CABA, ALEC, OABI, OPK, BVS | — | $75.84 | $9,964.00 | $10,039.84 | ARCT×75, BMEA×694, CRDL×578, HRMY×30, NVAX×121, PBH×23, PCRX×47, RVTY×9, CABA×5, ALEC×7, OABI×3, OPK×11, BVS×1 | BUY CABA x5 @ 3.63; BUY ALEC x7 @ 2.70; BUY OABI x3 @ 5.08; BUY OPK x11 @ 1.71; BUY BVS x1 @ 14.50 |

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
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $85.39 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 1 | $11.12 | $0.11 | — | $74.16 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AMPY` | 2 | $4.94 | $0.10 | — | $64.17 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $55.46 | rank by cond; rank cond; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 4 | $1.62 | $0.08 | — | $48.90 | rank by cond; rank cond; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $6.93 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `XHG` | 1 | $4.19 | $0.04 | — | $44.67 | rank by cond; rank cond; list yday_mover; ⚪; ret5=+291.8; leftover $6.93 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,242.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $2,410.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1543 | $1.14 | $20.17 | $+471.89 | $4,149.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $5,323.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,662.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $7,892.57 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $9,064.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $10,305.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $10,316.49 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 1 | $9.10 | $0.11 | $-2.25 | $10,325.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AMPY` | 2 | $4.88 | $0.12 | $-0.35 | $10,335.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $10,344.57 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 4 | $1.55 | $0.09 | $-0.45 | $10,350.67 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `XHG` | 1 | $4.10 | $0.06 | $-0.20 | $10,354.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $9,078.43 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,802.26 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 62 | $20.65 | $2.18 | — | $6,519.78 | rank by cond; rank cond; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 224 | $5.77 | $2.89 | — | $5,224.42 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,946.28 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,670.07 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 739 | $1.75 | $9.53 | — | $1,367.29 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.95 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1294.34 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $191.58 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $169.09 | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 10 | $2.47 | $0.28 | — | $144.11 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 13 | $1.93 | $0.29 | — | $118.73 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 19 | $1.32 | $0.31 | — | $93.35 | rank by cond; rank cond; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $26.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $1,376.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $2,717.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 62 | $20.85 | $2.20 | $+8.03 | $4,008.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 224 | $5.53 | $2.94 | $-59.59 | $5,243.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $6,647.69 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $8,054.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 739 | $1.91 | $9.67 | $+99.04 | $9,456.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $10,734.01 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `AU` | 11 | $119.46 | $2.02 | — | $9,417.93 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+25.9; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ERO` | 35 | $38.00 | $2.10 | — | $8,085.84 | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+16.6; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `FCX` | 17 | $77.90 | $2.04 | — | $6,759.50 | rank by cond; rank cond; list ohlc_hot,mover_buy; ⚪; ret5=+15.3; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CNH` | 114 | $11.72 | $2.33 | — | $5,421.08 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+13.7; leftover $1341.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HMY` | 59 | $22.65 | $2.17 | — | $4,082.57 | rank by cond; rank cond; list mover_buy; ⚪; ret5=+21.1; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $2,760.41 | rank by cond; rank cond; list flatten; ⚪; ret5=+13.0; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RHI` | 30 | $44.52 | $2.08 | — | $1,422.73 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+3.5; leftover $1341.75 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUZ` | 147 | $9.07 | $2.43 | — | $87.01 | rank by cond; rank cond; list mover_buy; ⚪; ret5=+8.3; leftover $1341.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $103.42 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 2 | $15.35 | $0.33 | $+7.88 | $133.79 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 10 | $2.41 | $0.29 | $-1.17 | $157.60 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 13 | $2.03 | $0.32 | $+0.69 | $183.66 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 19 | $1.60 | $0.38 | $+4.63 | $213.68 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 6 | $4.42 | $0.28 | — | $186.88 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-8.6; leftover $30.53 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 11 | $2.59 | $0.32 | — | $158.07 | rank by cond; rank cond; list flatten; ret5=+4.2; leftover $30.53 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 11 | $117.41 | $2.04 | $-26.62 | $1,447.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ERO` | 35 | $39.20 | $2.12 | $+37.79 | $2,817.42 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `FCX` | 17 | $78.83 | $2.06 | $+11.71 | $4,155.47 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 catal🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CNH` | 114 | $11.62 | $2.36 | $-16.09 | $5,477.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HMY` | 59 | $20.70 | $2.19 | $-119.40 | $6,696.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 55 | $24.00 | $2.18 | $-4.33 | $8,014.73 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RHI` | 30 | $44.41 | $2.10 | $-7.48 | $9,344.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUZ` | 147 | $8.88 | $2.47 | $-32.83 | $10,647.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `KEYS` | 4 | $323.82 | $2.00 | — | $9,350.54 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-11.7; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $8,153.32 | rank by cond; rank cond; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CIEN` | 3 | $411.53 | $2.00 | — | $6,916.74 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-7.7; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `MPWR` | 1 | $1319.75 | $1.99 | — | $5,594.99 | rank by cond; rank cond; list mover_buy; 🔵; ret5=-6.1; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `AVT` | 14 | $91.11 | $2.03 | — | $4,317.42 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-7.4; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `CGNX` | 21 | $62.80 | $2.05 | — | $2,996.57 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-7.8; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `COHR` | 4 | $303.67 | $2.00 | — | $1,779.89 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-11.1; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LSCC` | 10 | $121.13 | $2.02 | — | $566.57 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-9.8; leftover $1330.98 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 6 | $4.61 | $0.31 | $+0.54 | $593.91 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 11 | $2.70 | $0.35 | $+0.54 | $623.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `KEYS` | 4 | $321.47 | $2.02 | $-13.42 | $1,907.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $2,926.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CIEN` | 3 | $376.89 | $2.02 | $-107.94 | $4,054.78 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `MPWR` | 1 | $1245.11 | $2.01 | $-78.65 | $5,297.87 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `AVT` | 14 | $88.58 | $2.05 | $-39.50 | $6,535.94 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CGNX` | 21 | $59.72 | $2.07 | $-68.81 | $7,787.99 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `COHR` | 4 | $270.50 | $2.02 | $-136.70 | $8,867.97 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LSCC` | 10 | $113.60 | $2.04 | $-79.36 | $10,001.93 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 75 | $16.46 | $2.21 | — | $8,765.21 | rank by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `BMEA` | 694 | $1.80 | $8.95 | — | $7,507.06 | rank by cond; rank cond; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 578 | $2.16 | $7.46 | — | $6,251.12 | rank by cond; rank cond; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $5,009.74 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 121 | $10.27 | $2.35 | — | $3,764.72 | rank by cond; rank cond; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `PBH` | 23 | $52.88 | $2.06 | — | $2,546.42 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=-0.1; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `PCRX` | 47 | $26.52 | $2.13 | — | $1,297.85 | rank by cond; rank cond; list mover_buy; 🔵; ⚪; ret5=+7.3; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $162.37 | rank by cond; rank cond; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1250.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 5 | $3.63 | $0.20 | — | $144.03 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+13.8; leftover $20.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 7 | $2.70 | $0.21 | — | $124.92 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $20.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $5.08 | $0.16 | — | $109.51 | rank by cond; rank cond; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $20.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OPK` | 11 | $1.71 | $0.22 | — | $90.48 | rank by cond; rank cond; list yday_gainer; 🔵; ⚪; ret5=+7.2; leftover $20.30 | join🟢 sector🟢 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $75.84 | rank by cond; rank cond; list flatten; 🔵; ⚪; ret5=+0.8; leftover $20.30 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |

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
