# Factor mine action — `union_join_g_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ join_g, no 🚨

Cash book **+9.87%** ($10,987) · signal-only (no cash/fees) was +14.43%. Starts YES **12/17**. Fills 146 · skips 57 · realized $+694.13.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `join=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $418.62.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,055.59 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | BUY BTSG x20 @ 59.80; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY TGTX x25 @ 49.70; BUY SLS x106 @ 11.70; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY TNDM x53 @ 23.33 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $592.27 | $9,601.64 | $10,193.91 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | SELL BTSG (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); BUY TLN x3 @ 359.83; BUY VST x8 @ 146.90; BUY NRG x10 @ 120.00; BUY DAVE x3 @ 330.91; BUY SLG x22 @ 57.61; BUY MARA x140 @ 9.01; BUY LDI x1353 @ 0.94; BUY BTBT x845 @ 1.50 |
| 2026-08-17 | +2.25 | $592.27 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×22, MARA×140, LDI×1353, BTBT×845 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $189.31 | $9,947.87 | $10,137.18 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, NB×250 | SELL TLN (dropped from list after 1 sess (min 1)); SELL VST (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL DAVE (dropped from list after 1 sess (min 1)); SELL SLG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); BUY DVN x27 @ 46.18; BUY EOG x8 @ 142.77; BUY FANG x6 @ 202.70; BUY TMC x313 @ 4.05; BUY TGB x150 @ 8.46; BUY ELF x14 @ 90.54; BUY DNN x391 @ 3.24; BUY NB x250 @ 5.07 |
| 2026-08-18 | -6.20 | $189.31 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×391, NB×250 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | $10,036.07 | $0.00 | $10,036.07 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ELF (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL NB (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,036.07 | — | — | — | $10,036.07 | $0.00 | $10,036.07 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,036.07 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $193.11 | $10,051.63 | $10,244.74 | AG×61, BHP×13, CDE×60, HDSN×217, IAG×63, KGC×42, NFGC×716, WPM×8 | BUY AG x61 @ 20.55; BUY BHP x13 @ 91.01; BUY CDE x60 @ 20.65; BUY HDSN x217 @ 5.77; BUY IAG x63 @ 19.63; BUY KGC x42 @ 29.63; BUY NFGC x716 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $193.11 | AG×61, BHP×13, CDE×60, HDSN×217, IAG×63, KGC×42, NFGC×716, WPM×8 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $162.83 | $10,547.41 | $10,710.24 | AU×10, AUPH×76, AEM×6, ARCT×117, AUTL×530, CRDL×679, CRSP×21, CYPH×993 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x10 @ 119.43; BUY AUPH x76 @ 17.20; BUY AEM x6 @ 216.30; BUY ARCT x117 @ 11.13; BUY AUTL x530 @ 2.47; BUY CRDL x679 @ 1.93; BUY CRSP x21 @ 59.72; BUY CYPH x993 @ 1.32 |
| 2026-08-24 | -5.17 | $162.83 | AU×10, AUPH×76, AEM×6, ARCT×117, AUTL×530, CRDL×679, CRSP×21, CYPH×993 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,015.78 | $0.00 | $11,015.78 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $11,015.78 | — | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, ZURA | — | $106.56 | $10,897.65 | $11,004.21 | MOS×57, OCUL×126, INSP×22, CRMD×166, RZLT×263, HCA×3, BMEA×849, ZURA×215 | BUY MOS x57 @ 24.00; BUY OCUL x126 @ 10.92; BUY INSP x22 @ 61.47; BUY CRMD x166 @ 8.28; BUY RZLT x263 @ 5.23; BUY HCA x3 @ 429.24; BUY BMEA x849 @ 1.62; BUY ZURA x215 @ 6.38 |
| 2026-08-26 | +2.02 | $106.56 | MOS×57, OCUL×126, INSP×22, CRMD×166, RZLT×263, HCA×3, BMEA×849, ZURA×215 | — | — | $106.56 | $10,881.03 | $10,987.59 | MOS×57, OCUL×126, INSP×22, CRMD×166, RZLT×263, HCA×3, BMEA×849, ZURA×215 | hold MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,ZURA |
| 2026-08-27 | — | $106.56 | MOS×57, OCUL×126, INSP×22, CRMD×166, RZLT×263, HCA×3, BMEA×849, ZURA×215 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, BMEA, ZURA | $547.19 | $10,463.76 | $11,010.95 | MOS×57, RRC×33, CRK×97, SLI×529, ACMR×16, GGB×310, MT×18, MU×1 | SELL OCUL (dropped from list after 2 sess (min 1)); SELL INSP (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); SELL BMEA (dropped from list after 2 sess (min 1)); SELL ZURA (dropped from list after 2 sess (min 1)); BUY RRC x33 @ 40.72; BUY CRK x97 @ 14.09; BUY SLI x529 @ 2.59; BUY ACMR x16 @ 80.97; BUY GGB x310 @ 4.42; BUY MT x18 @ 75.12; BUY MU x1 @ 925.74 |
| 2026-08-28 | +0.75 | $547.19 | MOS×57, RRC×33, CRK×97, SLI×529, ACMR×16, GGB×310, MT×18, MU×1 | ANF, BZ, SEDG, SMTC, GRRR, URBN, VYX, SIMO | MOS, RRC, CRK, SLI, ACMR, GGB, MT, MU | $248.42 | $10,562.54 | $10,810.96 | ANF×9, BZ×74, SEDG×40, SMTC×9, GRRR×86, URBN×16, VYX×154, SIMO×5 | SELL MOS (dropped from list after 3 sess (min 1)); SELL RRC (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL SLI (dropped from list after 1 sess (min 1)); SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); BUY ANF x9 @ 144.70; BUY BZ x74 @ 18.50; BUY SEDG x40 @ 33.78; BUY SMTC x9 @ 149.40; BUY GRRR x86 @ 15.94; BUY URBN x16 @ 82.70; BUY VYX x154 @ 8.95; BUY SIMO x5 @ 272.00 |
| 2026-08-31 | -5.85 | $248.42 | ANF×9, BZ×74, SEDG×40, SMTC×9, GRRR×86, URBN×16, VYX×154, SIMO×5 | — | ANF, BZ, SEDG, SMTC, GRRR, URBN, VYX, SIMO | $10,508.53 | $0.00 | $10,508.53 | — | SELL ANF (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL SEDG (dropped from list after 1 sess (min 1)); SELL SMTC (dropped from list after 1 sess (min 1)); SELL GRRR (dropped from list after 1 sess (min 1)); SELL URBN (dropped from list after 1 sess (min 1)); SELL VYX (dropped from list after 1 sess (min 1)); SELL SIMO (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,508.53 | — | — | — | $10,508.53 | $0.00 | $10,508.53 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,508.53 | — | — | — | $10,508.53 | $0.00 | $10,508.53 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,508.53 | — | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MMED, CTMX | — | $116.98 | $10,757.67 | $10,874.65 | ATRC×26, HRMY×31, CABA×401, VSTM×170, RVTY×10, CRK×83, MMED×57, CTMX×353 | BUY ATRC x26 @ 49.76; BUY HRMY x31 @ 41.31; BUY CABA x401 @ 3.27; BUY VSTM x170 @ 7.70; BUY RVTY x10 @ 125.94; BUY CRK x83 @ 15.70; BUY MMED x57 @ 22.78; BUY CTMX x353 @ 3.72 |
| 2026-09-04 | — | $116.98 | ATRC×26, HRMY×31, CABA×401, VSTM×170, RVTY×10, CRK×83, MMED×57, CTMX×353 | ASND, OSCR, NVAX, BVS, SLBT, DELL | HRMY, VSTM, RVTY, CRK, MMED, CTMX | $418.62 | $10,568.82 | $10,987.44 | ATRC×26, CABA×401, ASND×5, OSCR×43, NVAX×129, BVS×92, SLBT×438, DELL×2 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL CTMX (dropped from list after 1 sess (min 1)); BUY ASND x5 @ 266.94; BUY OSCR x43 @ 30.65; BUY NVAX x129 @ 10.41; BUY BVS x92 @ 14.50; BUY SLBT x438 @ 3.07; BUY DELL x2 @ 486.31 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | ▼ $9,997.95 (-2.05) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | ▼ $9,995.88 (-4.12) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | ▼ $9,993.82 (-6.18) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | ▼ $9,991.75 (-8.25) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | ▼ $9,989.44 (-10.56) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | ▼ $9,987.33 (-12.67) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | ▼ $9,970.20 (-29.80) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | ▼ $9,968.05 (-31.95) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 20 | $59.65 | $2.07 | $-7.12 | $1,288.46 | ▲ $10,176.05 (+176.05) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 27 | $44.09 | $2.09 | $-55.19 | $2,476.80 | ▲ $10,173.96 (+173.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 24 | $55.29 | $2.08 | $+107.86 | $3,801.68 | ▲ $10,171.88 (+171.88) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 25 | $47.27 | $2.08 | $-64.90 | $4,981.35 | ▲ $10,169.80 (+169.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 106 | $12.40 | $2.34 | $+69.56 | $6,293.41 | ▲ $10,167.46 (+167.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 42 | $29.15 | $2.14 | $-29.03 | $7,515.57 | ▲ $10,165.32 (+165.32) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1543 | $0.93 | $19.25 | $+148.79 | $8,931.32 | ▲ $10,146.08 (+146.08) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 53 | $22.92 | $2.17 | $-26.05 | $10,143.91 | ▲ $10,143.91 (+143.91) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $9,062.42 | ▲ $10,141.91 (+141.91) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+5.9; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,885.21 | ▲ $10,139.90 (+139.90) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+3.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,683.19 | ▲ $10,137.88 (+137.88) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+0.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,688.46 | ▲ $10,135.88 (+135.88) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $4,418.98 | ▲ $10,133.82 (+133.82) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+5.7; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 140 | $9.01 | $2.41 | — | $3,155.17 | ▲ $10,131.41 (+131.41) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1353 | $0.94 | $16.74 | — | $1,870.67 | ▲ $10,114.67 (+114.67) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 845 | $1.50 | $10.90 | — | $592.27 | ▲ $10,103.77 (+103.77) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1267.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,693.89 | ▲ $10,194.18 (+194.18) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,886.82 | ▲ $10,192.15 (+192.15) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,158.78 | ▲ $10,190.11 (+190.11) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,167.58 | ▲ $10,188.09 (+188.09) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $6,383.64 | ▲ $10,186.02 (+186.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 140 | $9.22 | $2.44 | $+24.55 | $7,672.00 | ▲ $10,183.57 (+183.57) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1353 | $0.91 | $16.57 | $-73.89 | $8,882.61 | ▲ $10,167.01 (+167.01) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 845 | $1.52 | $11.05 | $-5.05 | $10,155.96 | ▲ $10,155.96 (+155.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,907.02 | ▲ $10,153.88 (+153.88) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+6.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,762.85 | ▲ $10,151.87 (+151.87) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+5.8; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,544.64 | ▲ $10,149.86 (+149.86) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+8.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,272.95 | ▲ $10,145.82 (+145.82) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,001.51 | ▲ $10,143.38 (+143.38) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,731.92 | ▲ $10,141.35 (+141.35) | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-7.2; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 391 | $3.24 | $5.04 | — | $1,460.04 | ▲ $10,136.31 (+136.31) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+0.3; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 250 | $5.07 | $3.23 | — | $189.31 | ▲ $10,133.08 (+133.08) | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=-4.7; leftover $1269.49 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,483.22 | ▲ $10,057.15 (+57.15) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.51 | ▲ $10,055.12 (+55.12) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,917.06 | ▲ $10,053.09 (+53.09) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,077.32 | ▲ $10,048.99 (+48.99) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,357.35 | ▲ $10,046.52 (+46.52) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.45 | ▲ $10,044.46 (+44.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 391 | $3.11 | $5.12 | $-60.99 | $8,874.34 | ▲ $10,039.34 (+39.34) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 250 | $4.66 | $3.28 | $-109.00 | $10,036.07 | ▲ $10,036.07 (+36.07) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,780.34 | ▲ $10,033.89 (+33.89) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,595.19 | ▲ $10,031.87 (+31.87) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 60 | $20.65 | $2.17 | — | $6,354.02 | ▲ $10,029.70 (+29.70) | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 217 | $5.77 | $2.80 | — | $5,099.13 | ▲ $10,026.90 (+26.90) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 63 | $19.63 | $2.18 | — | $3,860.26 | ▲ $10,024.72 (+24.72) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,613.68 | ▲ $10,022.60 (+22.60) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 716 | $1.75 | $9.24 | — | $1,351.45 | ▲ $10,013.37 (+13.37) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $193.11 | ▲ $10,011.35 (+11.35) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1254.51 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,526.82 | ▲ $10,510.66 (+510.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,769.13 | ▲ $10,508.61 (+508.61) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 60 | $21.75 | $2.19 | $+61.64 | $4,071.94 | ▲ $10,506.42 (+506.42) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 217 | $5.67 | $2.85 | $-27.34 | $5,299.48 | ▲ $10,503.57 (+503.57) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 63 | $21.17 | $2.20 | $+92.64 | $6,630.99 | ▲ $10,501.37 (+501.37) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $7,980.00 | ▲ $10,499.24 (+499.24) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 716 | $1.79 | $9.37 | $+10.04 | $9,252.27 | ▲ $10,489.87 (+489.87) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,487.84 | ▲ $10,487.84 (+487.84) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,291.52 | ▲ $10,485.82 (+485.82) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,982.10 | ▲ $10,483.60 (+483.60) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,682.29 | ▲ $10,481.59 (+481.59) | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 117 | $11.13 | $2.34 | — | $5,377.74 | ▲ $10,479.25 (+479.25) | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 530 | $2.47 | $6.84 | — | $4,061.80 | ▲ $10,472.41 (+472.41) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 679 | $1.93 | $8.76 | — | $2,742.57 | ▲ $10,463.65 (+463.65) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,486.40 | ▲ $10,461.60 (+461.60) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 993 | $1.32 | $12.81 | — | $162.83 | ▲ $10,448.79 (+448.79) | union ∩ join_g, no 🚨; gate join=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1310.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,365.79 | ▲ $11,053.30 (+1,053.30) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.60 | $2.24 | $-50.06 | $2,625.15 | ▲ $11,051.06 (+1,051.06) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,925.30 | ▲ $11,049.03 (+1,049.03) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 117 | $13.26 | $2.37 | $+244.50 | $5,474.35 | ▲ $11,046.66 (+1,046.66) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 530 | $2.36 | $6.93 | $-72.07 | $6,718.21 | ▲ $11,039.72 (+1,039.72) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 679 | $1.87 | $8.88 | $-58.38 | $7,979.06 | ▲ $11,030.84 (+1,030.84) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $9,211.58 | ▲ $11,028.77 (+1,028.77) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 993 | $1.83 | $12.99 | $+480.63 | $11,015.78 | ▲ $11,015.78 (+1,015.78) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $9,645.62 | ▲ $11,013.62 (+1,013.62) | union ∩ join_g, no 🚨; gate join=good; list flatten; ⚪; ret5=+13.0; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.92 | $2.37 | — | $8,267.33 | ▲ $11,011.25 (+1,011.25) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+10.4; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,912.93 | ▲ $11,009.19 (+1,009.19) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=+9.2; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 166 | $8.28 | $2.49 | — | $5,535.97 | ▲ $11,006.71 (+1,006.71) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 263 | $5.23 | $3.39 | — | $4,157.08 | ▲ $11,003.31 (+1,003.31) | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+10.7; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,867.36 | ▲ $11,001.31 (+1,001.31) | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+6.1; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 849 | $1.62 | $10.95 | — | $1,481.03 | ▲ $10,990.36 (+990.36) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 215 | $6.38 | $2.77 | — | $106.56 | ▲ $10,987.59 (+987.59) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1376.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 126 | $10.79 | $2.40 | $-21.15 | $1,463.70 | ▲ $11,032.55 (+1,032.55) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $2,783.16 | ▲ $11,030.47 (+1,030.47) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 166 | $8.60 | $2.53 | $+48.11 | $4,208.24 | ▲ $11,027.95 (+1,027.95) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 263 | $5.01 | $3.45 | $-64.70 | $5,522.42 | ▲ $11,024.50 (+1,024.50) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $6,802.90 | ▲ $11,022.48 (+1,022.48) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 849 | $1.75 | $11.10 | $+88.31 | $8,277.54 | ▲ $11,011.37 (+1,011.37) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 215 | $6.13 | $2.82 | $-59.34 | $9,592.67 | ▲ $11,008.55 (+1,008.55) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,246.83 | ▲ $11,006.47 (+1,006.47) | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+1.8; leftover $1370.38 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.09 | $2.28 | — | $6,877.81 | ▲ $11,004.18 (+1,004.18) | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+1.1; leftover $1370.38 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 529 | $2.59 | $6.82 | — | $5,500.88 | ▲ $10,997.36 (+997.36) | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+4.2; leftover $1370.38 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 16 | $80.97 | $2.04 | — | $4,203.32 | ▲ $10,995.32 (+995.32) | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=-1.3; leftover $1370.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 310 | $4.42 | $4.00 | — | $2,829.12 | ▲ $10,991.32 (+991.32) | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=-8.6; leftover $1370.38 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,474.92 | ▲ $10,989.28 (+989.28) | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=-2.2; leftover $1370.38 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $547.19 | ▲ $10,987.29 (+987.29) | union ∩ join_g, no 🚨; gate join=good; list mover_buy; 🔵; ret5=-0.5; leftover $1370.38 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `MOS` | 57 | $24.00 | $2.18 | $-4.34 | $1,913.00 | ▲ $11,086.49 (+1,086.49) | dropped from list after 3 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `RRC` | 33 | $41.44 | $2.11 | $+19.56 | $3,278.42 | ▲ $11,084.39 (+1,084.39) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `CRK` | 97 | $14.42 | $2.31 | $+27.42 | $4,674.85 | ▲ $11,082.08 (+1,082.08) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `SLI` | 529 | $2.60 | $6.92 | $-8.46 | $6,043.32 | ▲ $11,075.15 (+1,075.15) | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 16 | $81.65 | $2.06 | $+6.78 | $7,347.67 | ▲ $11,073.10 (+1,073.10) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 310 | $4.57 | $4.06 | $+38.44 | $8,760.30 | ▲ $11,069.03 (+1,069.03) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $74.54 | $2.06 | $-14.55 | $10,099.96 | ▲ $11,066.97 (+1,066.97) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $11,064.96 | ▲ $11,064.96 (+1,064.96) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,760.64 | ▲ $11,062.94 (+1,062.94) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1383.12 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 74 | $18.50 | $2.21 | — | $8,389.43 | ▲ $11,060.73 (+1,060.73) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1383.12 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $33.78 | $2.11 | — | $7,036.12 | ▲ $11,058.62 (+1,058.62) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1383.12 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $5,689.50 | ▲ $11,056.60 (+1,056.60) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1383.12 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 86 | $15.94 | $2.25 | — | $4,316.41 | ▲ $11,054.35 (+1,054.35) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1383.12 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $82.70 | $2.04 | — | $2,991.17 | ▲ $11,052.31 (+1,052.31) | union ∩ join_g, no 🚨; gate join=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1383.12 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 154 | $8.95 | $2.45 | — | $1,610.42 | ▲ $11,049.86 (+1,049.86) | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; 🔵; ret5=-3.1; leftover $1383.12 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 5 | $272.00 | $2.00 | — | $248.42 | ▲ $11,047.86 (+1,047.86) | union ∩ join_g, no 🚨; gate join=good; list yday_gainer; ⚪; ret5=-3.9; leftover $1383.12 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $1,584.41 | ▲ $10,523.78 (+523.78) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 74 | $17.89 | $2.23 | $-49.59 | $2,906.03 | ▲ $10,521.54 (+521.54) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.50 | $2.13 | $-95.44 | $4,163.90 | ▲ $10,519.41 (+519.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $133.04 | $2.04 | $-151.29 | $5,359.23 | ▲ $10,517.38 (+517.38) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 86 | $14.32 | $2.27 | $-143.84 | $6,588.47 | ▲ $10,515.10 (+515.10) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $81.09 | $2.06 | $-29.86 | $7,883.86 | ▲ $10,513.05 (+513.05) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 154 | $9.06 | $2.49 | $+12.00 | $9,276.61 | ▲ $10,510.56 (+510.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 5 | $246.79 | $2.02 | $-130.08 | $10,508.53 | ▲ $10,508.53 (+508.53) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $9,212.70 | ▲ $10,506.46 (+506.46) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1313.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $41.31 | $2.08 | — | $7,930.01 | ▲ $10,504.38 (+504.38) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1313.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 401 | $3.27 | $5.17 | — | $6,613.57 | ▲ $10,499.21 (+499.21) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1313.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 170 | $7.70 | $2.50 | — | $5,302.07 | ▲ $10,496.71 (+496.71) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1313.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,040.65 | ▲ $10,494.69 (+494.69) | union ∩ join_g, no 🚨; gate join=good; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1313.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 83 | $15.70 | $2.24 | — | $2,735.31 | ▲ $10,492.45 (+492.45) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1313.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $22.78 | $2.16 | — | $1,434.69 | ▲ $10,490.29 (+490.29) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1313.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 353 | $3.72 | $4.55 | — | $116.98 | ▲ $10,485.74 (+485.74) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1313.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 31 | $42.93 | $2.10 | $+46.03 | $1,445.70 | ▲ $10,926.01 (+926.01) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 170 | $8.03 | $2.54 | $+51.06 | $2,808.26 | ▲ $10,923.47 (+923.47) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,130.72 | ▲ $10,921.43 (+921.43) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 83 | $15.45 | $2.26 | $-25.25 | $5,410.81 | ▲ $10,919.17 (+919.17) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 57 | $23.88 | $2.18 | $+58.36 | $6,769.79 | ▲ $10,916.99 (+916.99) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 353 | $3.73 | $4.62 | $-5.65 | $8,081.85 | ▲ $10,912.36 (+912.36) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $6,745.15 | ▲ $10,910.36 (+910.36) | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+1.9; leftover $1346.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 43 | $30.65 | $2.12 | — | $5,425.08 | ▲ $10,908.24 (+908.24) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=-2.2; leftover $1346.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 129 | $10.41 | $2.38 | — | $4,079.81 | ▲ $10,905.86 (+905.86) | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1346.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 92 | $14.50 | $2.27 | — | $2,743.55 | ▲ $10,903.60 (+903.60) | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1346.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 438 | $3.07 | $5.65 | — | $1,393.24 | ▲ $10,897.95 (+897.95) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1346.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $418.62 | ▲ $10,895.95 (+895.95) | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1346.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ADI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `JKHY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `OCUL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INSP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRMD` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RZLT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `HCA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-26 | `MAIR` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 26 | 2026-09-03 @ $49.76 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1313.57 |
| `CABA` | 401 | 2026-09-03 @ $3.27 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1313.57 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | union ∩ join_g, no 🚨; gate join=good; list flatten; ret5=+1.9; leftover $1346.98 |
| `OSCR` | 43 | 2026-09-04 @ $30.65 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ret5=-2.2; leftover $1346.98 |
| `NVAX` | 129 | 2026-09-04 @ $10.41 | union ∩ join_g, no 🚨; gate join=good; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1346.98 |
| `BVS` | 92 | 2026-09-04 @ $14.50 | union ∩ join_g, no 🚨; gate join=good; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1346.98 |
| `SLBT` | 438 | 2026-09-04 @ $3.07 | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1346.98 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ join_g, no 🚨; gate join=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1346.98 |
