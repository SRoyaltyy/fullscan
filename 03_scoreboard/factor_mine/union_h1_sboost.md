# Factor mine action — `union_h1_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **+14.59%** ($11,459) · signal-only (no cash/fees) was +18.57%. Starts YES **16/17**. Fills 146 · skips 52 · realized $+1084.89.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $66.06.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $123.82 | $10,071.92 | $10,195.74 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | S=+8.53 more_names top_n=12; S=+8.53 sizeup x1.35; BUY BTSG x18 @ 59.80; BUY IREN x24 @ 45.98; BUY TPG x21 @ 50.62; BUY TGTX x22 @ 49.70; BUY SLS x94 @ 11.70; BUY HIMS x37 @ 29.74; BUY INO x1371 @ 0.81; BUY TNDM x47 @ 23.33; BUY VOR x50 @ 22.01 |
| 2026-08-14 | +5.50 | $123.82 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, WWW, HYLN | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | $458.79 | $9,693.39 | $10,152.17 | TLN×2, VST×5, NRG×7, DAVE×2, SLG×14, MARA×94, LDI×905, BTBT×565, BETR×57, ANGX×196, WWW×41, HYLN×203 | S=+5.50 more_names top_n=12; S=+5.50 sizeup x1.35; SELL BTSG (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); SELL VOR (dropped from list after 1 sess (min 1)); BUY TLN x2 @ 359.83; BUY VST x5 @ 146.90; BUY NRG x7 @ 120.00; BUY DAVE x2 @ 330.91; BUY SLG x14 @ 57.61; BUY MARA x94 @ 9.01; BUY LDI x905 @ 0.94; BUY BTBT x565 @ 1.50; BUY BETR x57 @ 14.80; BUY ANGX x196 @ 4.31; BUY WWW x41 @ 20.60; BUY HYLN x203 @ 4.18 |
| 2026-08-17 | +2.25 | $458.79 | TLN×2, VST×5, NRG×7, DAVE×2, SLG×14, MARA×94, LDI×905, BTBT×565, BETR×57, ANGX×196, WWW×41, HYLN×203 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT, BETR, ANGX, WWW, HYLN | $188.92 | $9,989.36 | $10,178.28 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×392, HNST×264 | SELL TLN (dropped from list after 1 sess (min 1)); SELL VST (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL DAVE (dropped from list after 1 sess (min 1)); SELL SLG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL BETR (dropped from list after 1 sess (min 1)); SELL ANGX (dropped from list after 1 sess (min 1)); SELL WWW (dropped from list after 1 sess (min 1)); SELL HYLN (dropped from list after 1 sess (min 1)); BUY DVN x27 @ 46.18; BUY EOG x8 @ 142.77; BUY FANG x6 @ 202.70; BUY TMC x313 @ 4.05; BUY TGB x150 @ 8.46; BUY ELF x14 @ 90.54; BUY DNN x392 @ 3.24; BUY HNST x264 @ 4.81 |
| 2026-08-18 | -6.20 | $188.92 | DVN×27, EOG×8, FANG×6, TMC×313, TGB×150, ELF×14, DNN×392, HNST×264 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,106.47 | $0.00 | $10,106.47 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ELF (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL HNST (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,106.47 | — | — | — | $10,106.47 | $0.00 | $10,106.47 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,106.47 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $208.63 | $10,107.56 | $10,316.19 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | BUY AG x61 @ 20.55; BUY BHP x13 @ 91.01; BUY CDE x61 @ 20.65; BUY HDSN x218 @ 5.77; BUY IAG x64 @ 19.63; BUY KGC x42 @ 29.63; BUY NFGC x721 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $208.63 | AG×61, BHP×13, CDE×61, HDSN×218, IAG×64, KGC×42, NFGC×721, WPM×8 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $18.48 | $10,768.60 | $10,787.08 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×1000 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x11 @ 119.43; BUY AUPH x76 @ 17.20; BUY AEM x6 @ 216.30; BUY ARCT x118 @ 11.13; BUY AUTL x534 @ 2.47; BUY CRDL x683 @ 1.93; BUY CRSP x22 @ 59.72; BUY CYPH x1000 @ 1.32 |
| 2026-08-24 | -5.17 | $18.48 | AU×11, AUPH×76, AEM×6, ARCT×118, AUTL×534, CRDL×683, CRSP×22, CYPH×1000 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $11,093.50 | $0.00 | $11,093.50 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $11,093.50 | — | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $135.25 | $10,928.53 | $11,063.78 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×693 | BUY MOS x57 @ 24.00; BUY OCUL x126 @ 10.92; BUY INSP x22 @ 61.47; BUY CRMD x167 @ 8.28; BUY RZLT x265 @ 5.23; BUY HCA x3 @ 429.24; BUY BMEA x855 @ 1.62; BUY NPWR x693 @ 2.00 |
| 2026-08-26 | +2.02 | $135.25 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×693 | — | — | $135.25 | $10,923.79 | $11,059.04 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×693 | hold MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,NPWR |
| 2026-08-27 | — | $135.25 | MOS×57, OCUL×126, INSP×22, CRMD×167, RZLT×265, HCA×3, BMEA×855, NPWR×693 | RRC, CRK, SLI, ACMR, GGB, MT, MU | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $517.93 | $10,562.23 | $11,080.16 | MOS×57, RRC×33, CRK×97, SLI×533, ACMR×17, GGB×312, MT×18, MU×1 | SELL OCUL (dropped from list after 2 sess (min 1)); SELL INSP (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); SELL BMEA (dropped from list after 2 sess (min 1)); SELL NPWR (dropped from list after 2 sess (min 1)); BUY RRC x33 @ 40.72; BUY CRK x97 @ 14.09; BUY SLI x533 @ 2.59; BUY ACMR x17 @ 80.97; BUY GGB x312 @ 4.42; BUY MT x18 @ 75.12; BUY MU x1 @ 925.74 |
| 2026-08-28 | +0.75 | $517.93 | MOS×57, RRC×33, CRK×97, SLI×533, ACMR×17, GGB×312, MT×18, MU×1 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT, MU | $100.18 | $11,110.59 | $11,210.77 | MOS×57, RRC×33, CRK×97, SLI×533, ANF×9, BHVN×83, BZ×76, CAPR×153 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); SELL MU (dropped from list after 1 sess (min 1)); BUY ANF x9 @ 144.70; BUY BHVN x83 @ 16.95; BUY BZ x76 @ 18.50; BUY CAPR x153 @ 9.19 |
| 2026-08-31 | -5.85 | $100.18 | MOS×57, RRC×33, CRK×97, SLI×533, ANF×9, BHVN×83, BZ×76, CAPR×153 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $10,961.62 | $0.00 | $10,961.62 | — | SELL MOS (dropped from list after 4 sess (min 1)); SELL RRC (dropped from list after 2 sess (min 1)); SELL CRK (dropped from list after 2 sess (min 1)); SELL SLI (dropped from list after 2 sess (min 1)); SELL ANF (dropped from list after 1 sess (min 1)); SELL BHVN (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,961.62 | — | — | — | $10,961.62 | $0.00 | $10,961.62 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,961.62 | — | — | — | $10,961.62 | $0.00 | $10,961.62 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,961.62 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $131.82 | $11,639.45 | $11,771.27 | ATRC×27, HRMY×33, CABA×419, VSTM×177, RVTY×10, GPRO×1123, FRVO×74, CRK×87 | BUY ATRC x27 @ 49.76; BUY HRMY x33 @ 41.31; BUY CABA x419 @ 3.27; BUY VSTM x177 @ 7.70; BUY RVTY x10 @ 125.94; BUY GPRO x1123 @ 1.22; BUY FRVO x74 @ 18.40; BUY CRK x87 @ 15.70 |
| 2026-09-04 | — | $131.82 | ATRC×27, HRMY×33, CABA×419, VSTM×177, RVTY×10, GPRO×1123, FRVO×74, CRK×87 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $66.06 | $11,393.13 | $11,459.19 | ATRC×27, CABA×419, GPRO×1123, ASND×5, OSCR×45, NVAX×134, BVS×96, BAK×715 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL FRVO (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); BUY ASND x5 @ 266.94; BUY OSCR x45 @ 30.65; BUY NVAX x134 @ 10.41; BUY BVS x96 @ 14.50; BUY BAK x715 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 18 | $59.80 | $2.04 | — | $8,921.56 | ▼ $9,997.96 (-2.04) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 24 | $45.98 | $2.06 | — | $7,815.97 | ▼ $9,995.89 (-4.11) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+12.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 21 | $50.62 | $2.05 | — | $6,750.83 | ▼ $9,993.84 (-6.16) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+6.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 22 | $49.70 | $2.06 | — | $5,655.38 | ▼ $9,991.78 (-8.22) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $4,553.31 | ▼ $9,989.51 (-10.49) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 37 | $29.74 | $2.10 | — | $3,450.82 | ▼ $9,987.41 (-12.59) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-5.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1371 | $0.81 | $15.22 | — | $2,325.10 | ▼ $9,972.19 (-27.81) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.2; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 47 | $23.33 | $2.13 | — | $1,226.46 | ▼ $9,970.06 (-29.94) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+19.7; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `VOR` | 50 | $22.01 | $2.14 | — | $123.82 | ▼ $9,967.92 (-32.08) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 18 | $59.65 | $2.06 | $-6.81 | $1,195.45 | ▲ $10,217.56 (+217.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 24 | $44.09 | $2.08 | $-49.50 | $2,251.53 | ▲ $10,215.48 (+215.48) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 21 | $55.29 | $2.07 | $+93.88 | $3,410.55 | ▲ $10,213.41 (+213.41) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 22 | $47.27 | $2.08 | $-57.59 | $4,448.41 | ▲ $10,211.33 (+211.33) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $5,611.71 | ▲ $10,209.03 (+209.03) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 37 | $29.15 | $2.12 | $-26.05 | $6,688.14 | ▲ $10,206.91 (+206.91) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 1371 | $0.93 | $17.10 | $+132.20 | $7,946.07 | ▲ $10,189.81 (+189.81) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 47 | $22.92 | $2.15 | $-23.55 | $9,021.16 | ▲ $10,187.66 (+187.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `VOR` | 50 | $23.33 | $2.16 | $+61.70 | $10,185.50 | ▲ $10,185.50 (+185.50) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 2 | $359.83 | $2.00 | — | $9,463.84 | ▲ $10,183.50 (+183.50) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.9; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 5 | $146.90 | $2.00 | — | $8,727.34 | ▲ $10,181.50 (+181.50) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+3.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 7 | $120.00 | $2.01 | — | $7,885.33 | ▲ $10,179.49 (+179.49) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+0.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 2 | $330.91 | $2.00 | — | $7,221.51 | ▲ $10,177.49 (+177.49) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-8.6; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 14 | $57.61 | $2.03 | — | $6,412.94 | ▲ $10,175.46 (+175.46) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.7; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 94 | $9.01 | $2.27 | — | $5,563.73 | ▲ $10,173.19 (+173.19) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-13.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 905 | $0.94 | $11.19 | — | $4,704.55 | ▲ $10,161.99 (+161.99) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 565 | $1.50 | $7.29 | — | $3,849.76 | ▲ $10,154.70 (+154.70) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 57 | $14.80 | $2.16 | — | $3,004.00 | ▲ $10,152.54 (+152.54) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-9.9; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 196 | $4.31 | $2.58 | — | $2,156.66 | ▲ $10,149.97 (+149.97) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 41 | $20.60 | $2.11 | — | $1,309.95 | ▲ $10,147.85 (+147.85) | S≥+5: sizeup + more names; list probable,yday_gainer; ret5=+4.4; leftover $848.79 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 203 | $4.18 | $2.62 | — | $458.79 | ▲ $10,145.23 (+145.23) | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $848.79 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 2 | $367.88 | $2.02 | $+12.09 | $1,192.53 | ▲ $10,199.83 (+199.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 5 | $149.37 | $2.02 | $+8.32 | $1,937.36 | ▲ $10,197.80 (+197.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 7 | $127.40 | $2.03 | $+47.76 | $2,827.13 | ▲ $10,195.77 (+195.77) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 2 | $336.94 | $2.02 | $+8.05 | $3,498.99 | ▲ $10,193.76 (+193.76) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 14 | $55.37 | $2.05 | $-35.44 | $4,272.12 | ▲ $10,191.70 (+191.70) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 94 | $9.22 | $2.30 | $+15.17 | $5,136.50 | ▲ $10,189.41 (+189.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 905 | $0.91 | $11.08 | $-49.43 | $5,946.25 | ▲ $10,178.32 (+178.32) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 565 | $1.52 | $7.39 | $-3.38 | $6,797.66 | ▲ $10,170.93 (+170.93) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 57 | $13.67 | $2.18 | $-68.75 | $7,574.67 | ▲ $10,168.75 (+168.75) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 196 | $4.60 | $2.62 | $+51.64 | $8,473.65 | ▲ $10,166.13 (+166.13) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 41 | $20.98 | $2.13 | $+11.33 | $9,331.70 | ▲ $10,164.00 (+164.00) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 203 | $4.10 | $2.66 | $-21.52 | $10,161.33 | ▲ $10,161.33 (+161.33) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,912.40 | ▲ $10,159.26 (+159.26) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+6.7; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,768.23 | ▲ $10,157.25 (+157.25) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+5.8; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,550.02 | ▲ $10,155.24 (+155.24) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+8.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 313 | $4.05 | $4.04 | — | $5,278.33 | ▲ $10,151.20 (+151.20) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 150 | $8.46 | $2.44 | — | $4,006.89 | ▲ $10,148.76 (+148.76) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 14 | $90.54 | $2.03 | — | $2,737.30 | ▲ $10,146.73 (+146.73) | S≥+5: sizeup + more names; list flatten; ret5=-7.2; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 392 | $3.24 | $5.06 | — | $1,462.16 | ▲ $10,141.67 (+141.67) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $1270.17 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 264 | $4.81 | $3.41 | — | $188.92 | ▲ $10,138.27 (+138.27) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-11.4; leftover $1270.17 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,482.83 | ▲ $10,127.75 (+127.75) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,665.11 | ▲ $10,125.71 (+125.71) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,916.67 | ▲ $10,123.69 (+123.69) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 313 | $3.72 | $4.10 | $-111.43 | $5,076.93 | ▲ $10,119.59 (+119.59) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 150 | $8.55 | $2.48 | $+8.58 | $6,356.95 | ▲ $10,117.11 (+117.11) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 14 | $93.44 | $2.05 | $+36.52 | $7,663.06 | ▲ $10,115.06 (+115.06) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 392 | $3.11 | $5.13 | $-61.15 | $8,877.05 | ▲ $10,109.93 (+109.93) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 264 | $4.67 | $3.46 | $-43.83 | $10,106.47 | ▲ $10,106.47 (+106.47) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,850.74 | ▲ $10,104.29 (+104.29) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,665.58 | ▲ $10,102.26 (+102.26) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,403.76 | ▲ $10,100.09 (+100.09) | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 218 | $5.77 | $2.81 | — | $5,143.09 | ▲ $10,097.28 (+97.28) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $3,884.59 | ▲ $10,095.10 (+95.10) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $2,638.01 | ▲ $10,092.98 (+92.98) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 721 | $1.75 | $9.30 | — | $1,366.96 | ▲ $10,083.68 (+83.68) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $208.63 | ▲ $10,081.67 (+81.67) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1263.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,542.33 | ▲ $10,583.71 (+583.71) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,784.64 | ▲ $10,581.66 (+581.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $4,109.20 | ▲ $10,579.47 (+579.47) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 218 | $5.67 | $2.86 | $-27.47 | $5,342.40 | ▲ $10,576.61 (+576.61) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $6,695.08 | ▲ $10,574.41 (+574.41) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $8,044.08 | ▲ $10,572.27 (+572.27) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 721 | $1.79 | $9.43 | $+10.11 | $9,325.24 | ▲ $10,562.84 (+562.84) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,560.81 | ▲ $10,560.81 (+560.81) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,245.05 | ▲ $10,558.78 (+558.78) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 76 | $17.20 | $2.22 | — | $7,935.64 | ▲ $10,556.57 (+556.57) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,635.83 | ▲ $10,554.56 (+554.56) | S≥+5: sizeup + more names; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 118 | $11.13 | $2.34 | — | $5,320.14 | ▲ $10,552.21 (+552.21) | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 534 | $2.47 | $6.89 | — | $3,994.27 | ▲ $10,545.32 (+545.32) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 683 | $1.93 | $8.81 | — | $2,667.27 | ▲ $10,536.51 (+536.51) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 22 | $59.72 | $2.06 | — | $1,351.38 | ▲ $10,534.46 (+534.46) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1000 | $1.32 | $12.90 | — | $18.48 | ▲ $10,521.56 (+521.56) | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1320.10 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.50 | $2.04 | $+7.70 | $1,341.93 | ▲ $11,131.22 (+1,131.22) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 76 | $16.60 | $2.24 | $-50.06 | $2,601.29 | ▲ $11,128.98 (+1,128.98) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,901.45 | ▲ $11,126.96 (+1,126.96) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 118 | $13.26 | $2.38 | $+246.62 | $5,463.75 | ▲ $11,124.58 (+1,124.58) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 534 | $2.36 | $6.99 | $-72.62 | $6,717.00 | ▲ $11,117.59 (+1,117.59) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 683 | $1.87 | $8.93 | $-58.72 | $7,985.28 | ▲ $11,108.66 (+1,108.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 22 | $58.79 | $2.08 | $-24.59 | $9,276.58 | ▲ $11,106.58 (+1,106.58) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1000 | $1.83 | $13.08 | $+484.02 | $11,093.50 | ▲ $11,093.50 (+1,093.50) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $9,723.34 | ▲ $11,091.34 (+1,091.34) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+13.0; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 126 | $10.92 | $2.37 | — | $8,345.05 | ▲ $11,088.97 (+1,088.97) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+10.4; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $6,990.66 | ▲ $11,086.92 (+1,086.92) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+9.2; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 167 | $8.28 | $2.49 | — | $5,605.40 | ▲ $11,084.42 (+1,084.42) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 265 | $5.23 | $3.42 | — | $4,216.04 | ▲ $11,081.01 (+1,081.01) | S≥+5: sizeup + more names; list flatten; ret5=+10.7; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,926.32 | ▲ $11,079.01 (+1,079.01) | S≥+5: sizeup + more names; list flatten; ret5=+6.1; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 855 | $1.62 | $11.03 | — | $1,530.19 | ▲ $11,067.98 (+1,067.98) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1386.69 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 693 | $2.00 | $8.94 | — | $135.25 | ▲ $11,059.04 (+1,059.04) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1386.69 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 126 | $10.79 | $2.40 | $-21.15 | $1,492.39 | ▲ $11,109.90 (+1,109.90) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $2,811.85 | ▲ $11,107.82 (+1,107.82) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 167 | $8.60 | $2.53 | $+48.42 | $4,245.52 | ▲ $11,105.29 (+1,105.29) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 265 | $5.01 | $3.47 | $-65.19 | $5,569.70 | ▲ $11,101.82 (+1,101.82) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $6,850.18 | ▲ $11,099.80 (+1,099.80) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 855 | $1.75 | $11.18 | $+88.94 | $8,335.25 | ▲ $11,088.62 (+1,088.62) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 693 | $1.93 | $9.07 | $-66.52 | $9,663.67 | ▲ $11,079.55 (+1,079.55) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,317.82 | ▲ $11,077.46 (+1,077.46) | S≥+5: sizeup + more names; list flatten; ret5=+1.8; leftover $1380.52 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 97 | $14.09 | $2.28 | — | $6,948.81 | ▲ $11,075.18 (+1,075.18) | S≥+5: sizeup + more names; list flatten; ret5=+1.1; leftover $1380.52 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 533 | $2.59 | $6.88 | — | $5,561.46 | ▲ $11,068.30 (+1,068.30) | S≥+5: sizeup + more names; list flatten; ret5=+4.2; leftover $1380.52 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $80.97 | $2.04 | — | $4,182.93 | ▲ $11,066.26 (+1,066.26) | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-1.3; leftover $1380.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 312 | $4.42 | $4.02 | — | $2,799.87 | ▲ $11,062.24 (+1,062.24) | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-8.6; leftover $1380.52 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,445.66 | ▲ $11,060.19 (+1,060.19) | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-2.2; leftover $1380.52 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $517.93 | ▲ $11,058.20 (+1,058.20) | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-0.5; leftover $1380.52 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $81.65 | $2.06 | $+7.46 | $1,903.92 | ▲ $11,158.55 (+1,158.55) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 312 | $4.57 | $4.09 | $+38.69 | $3,325.67 | ▲ $11,154.46 (+1,154.46) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $74.54 | $2.06 | $-14.55 | $4,665.33 | ▲ $11,152.40 (+1,152.40) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $5,630.32 | ▲ $11,150.38 (+1,150.38) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,326.01 | ▲ $11,148.37 (+1,148.37) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1407.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 83 | $16.95 | $2.24 | — | $2,916.92 | ▲ $11,146.13 (+1,146.13) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1407.58 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 76 | $18.50 | $2.22 | — | $1,508.70 | ▲ $11,143.91 (+1,143.91) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1407.58 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 153 | $9.19 | $2.45 | — | $100.18 | ▲ $11,141.46 (+1,141.46) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1407.58 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.75 | $2.18 | $-18.59 | $1,451.75 | ▲ $10,982.04 (+982.04) | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,806.27 | ▲ $10,979.93 (+979.93) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 97 | $14.56 | $2.31 | $+41.00 | $4,216.28 | ▲ $10,977.62 (+977.62) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 533 | $2.51 | $6.97 | $-56.49 | $5,547.14 | ▲ $10,970.65 (+970.65) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,883.13 | ▲ $10,968.61 (+968.61) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 83 | $15.44 | $2.26 | $-129.83 | $8,162.38 | ▲ $10,966.34 (+966.34) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 76 | $17.89 | $2.24 | $-50.82 | $9,519.78 | ▲ $10,964.10 (+964.10) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 153 | $9.44 | $2.49 | $+33.31 | $10,961.62 | ▲ $10,961.62 (+961.62) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,616.03 | ▲ $10,959.55 (+959.55) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 33 | $41.31 | $2.09 | — | $8,250.71 | ▲ $10,957.46 (+957.46) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 419 | $3.27 | $5.41 | — | $6,875.17 | ▲ $10,952.05 (+952.05) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 177 | $7.70 | $2.52 | — | $5,509.75 | ▲ $10,949.53 (+949.53) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,248.33 | ▲ $10,947.51 (+947.51) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1370.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1123 | $1.22 | $14.49 | — | $2,863.78 | ▲ $10,933.02 (+933.02) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1370.20 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 74 | $18.40 | $2.21 | — | $1,499.97 | ▲ $10,930.81 (+930.81) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1370.20 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 87 | $15.70 | $2.25 | — | $131.82 | ▲ $10,928.56 (+928.56) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1370.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 33 | $42.93 | $2.11 | $+49.26 | $1,546.40 | ▲ $11,936.01 (+1,936.01) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 177 | $8.03 | $2.56 | $+53.33 | $2,965.15 | ▲ $11,933.45 (+1,933.45) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,287.61 | ▲ $11,931.41 (+1,931.41) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 74 | $18.27 | $2.24 | $-14.07 | $5,637.35 | ▲ $11,929.17 (+1,929.17) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 87 | $15.45 | $2.28 | $-26.28 | $6,979.23 | ▲ $11,926.90 (+1,926.90) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 5 | $266.94 | $2.00 | — | $5,642.52 | ▲ $11,924.89 (+1,924.89) | S≥+5: sizeup + more names; list flatten; ret5=+1.9; leftover $1395.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 45 | $30.65 | $2.12 | — | $4,261.15 | ▲ $11,922.77 (+1,922.77) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-2.2; leftover $1395.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 134 | $10.41 | $2.39 | — | $2,863.82 | ▲ $11,920.38 (+1,920.38) | S≥+5: sizeup + more names; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1395.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 96 | $14.50 | $2.28 | — | $1,469.54 | ▲ $11,918.10 (+1,918.10) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1395.85 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 715 | $1.95 | $9.22 | — | $66.06 | ▲ $11,908.87 (+1,908.87) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1395.85 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 27 | 2026-09-03 @ $49.76 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1370.20 |
| `CABA` | 419 | 2026-09-03 @ $3.27 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1370.20 |
| `GPRO` | 1123 | 2026-09-03 @ $1.22 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1370.20 |
| `ASND` | 5 | 2026-09-04 @ $266.94 | S≥+5: sizeup + more names; list flatten; ret5=+1.9; leftover $1395.85 |
| `OSCR` | 45 | 2026-09-04 @ $30.65 | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-2.2; leftover $1395.85 |
| `NVAX` | 134 | 2026-09-04 @ $10.41 | S≥+5: sizeup + more names; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1395.85 |
| `BVS` | 96 | 2026-09-04 @ $14.50 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1395.85 |
| `BAK` | 715 | 2026-09-04 @ $1.95 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1395.85 |
