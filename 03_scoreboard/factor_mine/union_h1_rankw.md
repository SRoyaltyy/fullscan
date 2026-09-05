# Factor mine action — `union_h1_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **+13.90%** ($11,391) · signal-only (no cash/fees) was +18.57%. Starts YES **16/17**. Fills 134 · skips 53 · realized $+958.98.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $121.88.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $128.05 | $9,988.98 | $10,117.03 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | BUY BTSG x37 @ 59.80; BUY IREN x42 @ 45.98; BUY TPG x32 @ 50.62; BUY TGTX x27 @ 49.70; BUY SLS x94 @ 11.70; BUY HIMS x28 @ 29.74; BUY INO x685 @ 0.81; BUY TNDM x11 @ 23.33 |
| 2026-08-14 | +5.50 | $128.05 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $334.42 | $9,830.37 | $10,164.79 | TLN×6, VST×13, NRG×13, DAVE×4, SLG×19, MARA×93, LDI×597, BTBT×186 | SELL BTSG (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL TGTX (dropped from list after 1 sess (min 1)); SELL SLS (dropped from list after 1 sess (min 1)); SELL HIMS (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); BUY TLN x6 @ 359.83; BUY VST x13 @ 146.90; BUY NRG x13 @ 120.00; BUY DAVE x4 @ 330.91; BUY SLG x19 @ 57.61; BUY MARA x93 @ 9.01; BUY LDI x597 @ 0.94; BUY BTBT x186 @ 1.50 |
| 2026-08-17 | +2.25 | $334.42 | TLN×6, VST×13, NRG×13, DAVE×4, SLG×19, MARA×93, LDI×597, BTBT×186 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $242.31 | $10,033.94 | $10,276.25 | DVN×49, EOG×13, FANG×8, TMC×349, TGB×133, ELF×9, DNN×174, HNST×58 | SELL TLN (dropped from list after 1 sess (min 1)); SELL VST (dropped from list after 1 sess (min 1)); SELL NRG (dropped from list after 1 sess (min 1)); SELL DAVE (dropped from list after 1 sess (min 1)); SELL SLG (dropped from list after 1 sess (min 1)); SELL MARA (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); BUY DVN x49 @ 46.18; BUY EOG x13 @ 142.77; BUY FANG x8 @ 202.70; BUY TMC x349 @ 4.05; BUY TGB x133 @ 8.46; BUY ELF x9 @ 90.54; BUY DNN x174 @ 3.24; BUY HNST x58 @ 4.81 |
| 2026-08-18 | -6.20 | $242.31 | DVN×49, EOG×13, FANG×8, TMC×349, TGB×133, ELF×9, DNN×174, HNST×58 | — | DVN, EOG, FANG, TMC, TGB, ELF, DNN, HNST | $10,258.63 | $0.00 | $10,258.63 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL TMC (dropped from list after 1 sess (min 1)); SELL TGB (dropped from list after 1 sess (min 1)); SELL ELF (dropped from list after 1 sess (min 1)); SELL DNN (dropped from list after 1 sess (min 1)); SELL HNST (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,258.63 | — | — | — | $10,258.63 | $0.00 | $10,258.63 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,258.63 | — | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $273.07 | $10,186.41 | $10,459.48 | AG×110, BHP×21, CDE×82, HDSN×246, IAG×58, KGC×28, NFGC×325, WPM×1 | BUY AG x110 @ 20.55; BUY BHP x21 @ 91.01; BUY CDE x82 @ 20.65; BUY HDSN x246 @ 5.77; BUY IAG x58 @ 19.63; BUY KGC x28 @ 29.63; BUY NFGC x325 @ 1.75; BUY WPM x1 @ 144.54 |
| 2026-08-21 | +3.25 | $273.07 | AG×110, BHP×21, CDE×82, HDSN×246, IAG×58, KGC×28, NFGC×325, WPM×1 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $211.91 | $10,711.10 | $10,923.01 | AU×19, AUPH×121, AEM×8, ARCT×133, AUTL×482, CRDL×462, CRSP×9, CYPH×225 | SELL AG (dropped from list after 1 sess (min 1)); SELL BHP (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); BUY AU x19 @ 119.43; BUY AUPH x121 @ 17.20; BUY AEM x8 @ 216.30; BUY ARCT x133 @ 11.13; BUY AUTL x482 @ 2.47; BUY CRDL x462 @ 1.93; BUY CRSP x9 @ 59.72; BUY CYPH x225 @ 1.32 |
| 2026-08-24 | -5.17 | $211.91 | AU×19, AUPH×121, AEM×8, ARCT×133, AUTL×482, CRDL×462, CRSP×9, CYPH×225 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,925.88 | $0.00 | $10,925.88 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL AUTL (dropped from list after 1 sess (min 1)); SELL CRDL (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,925.88 | — | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | — | $84.08 | $10,806.54 | $10,890.62 | MOS×101, OCUL×194, INSP×29, CRMD×183, RZLT×232, HCA×2, BMEA×374, NPWR×151 | BUY MOS x101 @ 24.00; BUY OCUL x194 @ 10.92; BUY INSP x29 @ 61.47; BUY CRMD x183 @ 8.28; BUY RZLT x232 @ 5.23; BUY HCA x2 @ 429.24; BUY BMEA x374 @ 1.62; BUY NPWR x151 @ 2.00 |
| 2026-08-26 | +2.02 | $84.08 | MOS×101, OCUL×194, INSP×29, CRMD×183, RZLT×232, HCA×2, BMEA×374, NPWR×151 | — | — | $84.08 | $10,820.07 | $10,904.15 | MOS×101, OCUL×194, INSP×29, CRMD×183, RZLT×232, HCA×2, BMEA×374, NPWR×151 | hold MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,NPWR |
| 2026-08-27 | — | $84.08 | MOS×101, OCUL×194, INSP×29, CRMD×183, RZLT×232, HCA×2, BMEA×374, NPWR×151 | RRC, CRK, SLI, ACMR, GGB, MT | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $396.74 | $10,545.24 | $10,941.98 | MOS×101, RRC×51, CRK×128, SLI×581, ACMR×14, GGB×204, MT×8 | SELL OCUL (dropped from list after 2 sess (min 1)); SELL INSP (dropped from list after 2 sess (min 1)); SELL CRMD (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); SELL BMEA (dropped from list after 2 sess (min 1)); SELL NPWR (dropped from list after 2 sess (min 1)); BUY RRC x51 @ 40.72; BUY CRK x128 @ 14.09; BUY SLI x581 @ 2.59; BUY ACMR x14 @ 80.97; BUY GGB x204 @ 4.42; BUY MT x8 @ 75.12 |
| 2026-08-28 | +0.75 | $396.74 | MOS×101, RRC×51, CRK×128, SLI×581, ACMR×14, GGB×204, MT×8 | ANF, BHVN, BZ, CAPR | ACMR, GGB, MT | $66.67 | $10,891.06 | $10,957.73 | MOS×101, RRC×51, CRK×128, SLI×581, ANF×8, BHVN×54, BZ×33, CAPR×33 | SELL ACMR (dropped from list after 1 sess (min 1)); SELL GGB (dropped from list after 1 sess (min 1)); SELL MT (dropped from list after 1 sess (min 1)); BUY ANF x8 @ 144.70; BUY BHVN x54 @ 16.95; BUY BZ x33 @ 18.50; BUY CAPR x33 @ 9.19 |
| 2026-08-31 | -5.85 | $66.67 | MOS×101, RRC×51, CRK×128, SLI×581, ANF×8, BHVN×54, BZ×33, CAPR×33 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, CAPR | $10,786.09 | $0.00 | $10,786.09 | — | SELL MOS (dropped from list after 4 sess (min 1)); SELL RRC (dropped from list after 2 sess (min 1)); SELL CRK (dropped from list after 2 sess (min 1)); SELL SLI (dropped from list after 2 sess (min 1)); SELL ANF (dropped from list after 1 sess (min 1)); SELL BHVN (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL CAPR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,786.09 | — | — | — | $10,786.09 | $0.00 | $10,786.09 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,786.09 | — | — | — | $10,786.09 | $0.00 | $10,786.09 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,786.09 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $95.03 | $11,476.05 | $11,571.08 | ATRC×48, HRMY×50, CABA×549, VSTM×194, RVTY×9, GPRO×736, FRVO×32, CRK×19 | BUY ATRC x48 @ 49.76; BUY HRMY x50 @ 41.31; BUY CABA x549 @ 3.27; BUY VSTM x194 @ 7.70; BUY RVTY x9 @ 125.94; BUY GPRO x736 @ 1.22; BUY FRVO x32 @ 18.40; BUY CRK x19 @ 15.70 |
| 2026-09-04 | — | $95.03 | ATRC×48, HRMY×50, CABA×549, VSTM×194, RVTY×9, GPRO×736, FRVO×32, CRK×19 | ASND, OSCR, NVAX, BVS, BAK | HRMY, VSTM, RVTY, FRVO, CRK | $121.88 | $11,268.64 | $11,390.52 | ATRC×48, CABA×549, GPRO×736, ASND×7, OSCR×50, NVAX×112, BVS×53, BAK×200 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL FRVO (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); BUY ASND x7 @ 266.94; BUY OSCR x50 @ 30.65; BUY NVAX x112 @ 10.41; BUY BVS x53 @ 14.50; BUY BAK x200 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 37 | $59.80 | $2.10 | — | $7,785.30 | ▼ $9,997.90 (-2.10) | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $2222.22 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 42 | $45.98 | $2.12 | — | $5,852.02 | ▼ $9,995.78 (-4.22) | rank-weighted leftover; list flatten; ⚪; ret5=+12.3; leftover $1944.44 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 32 | $50.62 | $2.09 | — | $4,229.99 | ▼ $9,993.70 (-6.30) | rank-weighted leftover; list flatten; ⚪; ret5=+6.2; leftover $1666.67 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 27 | $49.70 | $2.07 | — | $2,886.02 | ▼ $9,991.63 (-8.37) | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1388.89 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 94 | $11.70 | $2.27 | — | $1,783.95 | ▼ $9,989.35 (-10.65) | rank-weighted leftover; list flatten; ⚪; ret5=-0.8; leftover $1111.11 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $949.16 | ▼ $9,987.28 (-12.72) | rank-weighted leftover; list flatten; ⚪; ret5=-5.3; leftover $833.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 685 | $0.81 | $7.60 | — | $386.70 | ▼ $9,979.68 (-20.32) | rank-weighted leftover; list flatten; ⚪; ret5=+13.2; leftover $555.56 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 11 | $23.33 | $2.02 | — | $128.05 | ▼ $9,977.65 (-22.35) | rank-weighted leftover; list flatten; ⚪; ret5=+19.7; leftover $277.78 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 37 | $59.65 | $2.13 | $-9.78 | $2,332.97 | ▲ $10,101.29 (+101.29) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 42 | $44.09 | $2.14 | $-83.64 | $4,182.61 | ▲ $10,099.15 (+99.15) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 32 | $55.29 | $2.11 | $+145.14 | $5,949.78 | ▲ $10,097.04 (+97.04) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TGTX` | 27 | $47.27 | $2.09 | $-69.77 | $7,223.98 | ▲ $10,094.95 (+94.95) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `SLS` | 94 | $12.40 | $2.30 | $+61.23 | $8,387.28 | ▲ $10,092.65 (+92.65) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `HIMS` | 28 | $29.15 | $2.09 | $-20.69 | $9,201.39 | ▲ $10,090.56 (+90.56) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 685 | $0.93 | $8.55 | $+66.05 | $9,829.89 | ▲ $10,082.01 (+82.01) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 11 | $22.92 | $2.04 | $-8.58 | $10,079.97 | ▲ $10,079.97 (+79.97) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 6 | $359.83 | $2.01 | — | $7,918.98 | ▲ $10,077.96 (+77.96) | rank-weighted leftover; list flatten; 🔵; ret5=+5.9; leftover $2239.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 13 | $146.90 | $2.03 | — | $6,007.25 | ▲ $10,075.93 (+75.93) | rank-weighted leftover; list flatten; 🔵; ret5=+3.6; leftover $1959.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 13 | $120.00 | $2.03 | — | $4,445.22 | ▲ $10,073.90 (+73.90) | rank-weighted leftover; list flatten; 🔵; ret5=+0.6; leftover $1679.99 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 4 | $330.91 | $2.00 | — | $3,119.58 | ▲ $10,071.90 (+71.90) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1400.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 19 | $57.61 | $2.05 | — | $2,022.94 | ▲ $10,069.85 (+69.85) | rank-weighted leftover; list flatten; 🔵; ret5=+5.7; leftover $1120.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 93 | $9.01 | $2.27 | — | $1,182.74 | ▲ $10,067.58 (+67.58) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $840.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 597 | $0.94 | $7.38 | — | $615.97 | ▲ $10,060.20 (+60.20) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $560.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 186 | $1.50 | $2.55 | — | $334.42 | ▲ $10,057.65 (+57.65) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $280.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 6 | $367.88 | $2.04 | $+44.26 | $2,539.66 | ▲ $10,219.12 (+219.12) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 13 | $149.37 | $2.05 | $+28.03 | $4,479.42 | ▲ $10,217.07 (+217.07) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 13 | $127.40 | $2.05 | $+92.12 | $6,133.57 | ▲ $10,215.02 (+215.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 4 | $336.94 | $2.02 | $+20.10 | $7,479.31 | ▲ $10,212.99 (+212.99) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 19 | $55.37 | $2.07 | $-46.67 | $8,529.27 | ▲ $10,210.93 (+210.93) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 93 | $9.22 | $2.29 | $+14.97 | $9,384.43 | ▲ $10,208.63 (+208.63) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 597 | $0.91 | $7.31 | $-32.61 | $9,918.60 | ▲ $10,201.32 (+201.32) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 186 | $1.52 | $2.59 | $-1.42 | $10,198.73 | ▲ $10,198.73 (+198.73) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 49 | $46.18 | $2.14 | — | $7,933.77 | ▲ $10,196.59 (+196.59) | rank-weighted leftover; list flatten; 🔵; ret5=+6.7; leftover $2266.38 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 13 | $142.77 | $2.03 | — | $6,075.73 | ▲ $10,194.56 (+194.56) | rank-weighted leftover; list flatten; 🔵; ret5=+5.8; leftover $1983.09 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 8 | $202.70 | $2.01 | — | $4,452.12 | ▲ $10,192.55 (+192.55) | rank-weighted leftover; list flatten; 🔵; ret5=+8.3; leftover $1699.79 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 349 | $4.05 | $4.50 | — | $3,034.17 | ▲ $10,188.05 (+188.05) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1416.49 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 133 | $8.46 | $2.39 | — | $1,906.60 | ▲ $10,185.66 (+185.66) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1133.19 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 9 | $90.54 | $2.02 | — | $1,089.72 | ▲ $10,183.64 (+183.64) | rank-weighted leftover; list flatten; ret5=-7.2; leftover $849.89 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 174 | $3.24 | $2.51 | — | $523.45 | ▲ $10,181.13 (+181.13) | rank-weighted leftover; list flatten; ⚪; ret5=+0.3; leftover $566.60 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 58 | $4.81 | $2.16 | — | $242.31 | ▲ $10,178.97 (+178.97) | rank-weighted leftover; list flatten; ⚪; ret5=-11.4; leftover $283.30 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 49 | $48.00 | $2.17 | $+84.88 | $2,592.14 | ▲ $10,276.49 (+276.49) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 13 | $148.04 | $2.05 | $+64.43 | $4,514.60 | ▲ $10,274.43 (+274.43) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 8 | $208.93 | $2.04 | $+45.79 | $6,184.01 | ▲ $10,272.40 (+272.40) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 349 | $3.72 | $4.57 | $-124.24 | $7,477.72 | ▲ $10,267.83 (+267.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 133 | $8.55 | $2.42 | $+7.16 | $8,612.45 | ▲ $10,265.41 (+265.41) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ELF` | 9 | $93.44 | $2.04 | $+22.05 | $9,451.37 | ▲ $10,263.37 (+263.37) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `DNN` | 174 | $3.11 | $2.55 | $-27.68 | $9,989.96 | ▲ $10,260.82 (+260.82) | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 vol🟢 buy🟢 |
| 2026-08-18 09:30 ET | **SELL** | `HNST` | 58 | $4.67 | $2.18 | $-12.47 | $10,258.63 | ▲ $10,258.63 (+258.63) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 110 | $20.55 | $2.32 | — | $7,995.81 | ▲ $10,256.31 (+256.31) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2279.70 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 21 | $91.01 | $2.05 | — | $6,082.55 | ▲ $10,254.26 (+254.26) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1994.73 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 82 | $20.65 | $2.24 | — | $4,387.02 | ▲ $10,252.03 (+252.03) | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1709.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 246 | $5.77 | $3.17 | — | $2,964.42 | ▲ $10,248.85 (+248.85) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1424.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 58 | $19.63 | $2.16 | — | $1,823.72 | ▲ $10,246.69 (+246.69) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1139.85 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 28 | $29.63 | $2.07 | — | $992.00 | ▲ $10,244.61 (+244.61) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $854.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 325 | $1.75 | $4.19 | — | $419.06 | ▲ $10,240.42 (+240.42) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $569.92 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 1 | $144.54 | $1.45 | — | $273.07 | ▲ $10,238.97 (+238.97) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $284.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 110 | $21.90 | $2.36 | $+143.82 | $2,679.72 | ▲ $10,733.23 (+733.23) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 21 | $95.72 | $2.08 | $+94.78 | $4,687.76 | ▲ $10,731.15 (+731.15) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 82 | $21.75 | $2.26 | $+85.70 | $6,468.99 | ▲ $10,728.88 (+728.88) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 246 | $5.67 | $3.23 | $-31.00 | $7,860.59 | ▲ $10,725.66 (+725.66) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 58 | $21.17 | $2.18 | $+84.97 | $9,086.26 | ▲ $10,723.47 (+723.47) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 28 | $32.17 | $2.09 | $+66.95 | $9,984.93 | ▲ $10,721.38 (+721.38) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 325 | $1.79 | $4.26 | $+4.55 | $10,562.42 | ▲ $10,717.12 (+717.12) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 1 | $154.70 | $1.57 | $+7.14 | $10,715.55 | ▲ $10,715.55 (+715.55) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 19 | $119.43 | $2.05 | — | $8,444.34 | ▲ $10,713.51 (+713.51) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $2381.23 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 121 | $17.20 | $2.35 | — | $6,360.78 | ▲ $10,711.15 (+711.15) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $2083.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 8 | $216.30 | $2.01 | — | $4,628.37 | ▲ $10,709.14 (+709.14) | rank-weighted leftover; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1785.93 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 133 | $11.13 | $2.39 | — | $3,145.69 | ▲ $10,706.75 (+706.75) | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1488.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 482 | $2.47 | $6.22 | — | $1,948.93 | ▲ $10,700.53 (+700.53) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1190.62 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 462 | $1.93 | $5.96 | — | $1,051.31 | ▲ $10,694.57 (+694.57) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $892.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 9 | $59.72 | $2.02 | — | $511.81 | ▲ $10,692.55 (+692.55) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $595.31 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 225 | $1.32 | $2.90 | — | $211.91 | ▲ $10,689.65 (+689.65) | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $297.65 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 19 | $120.50 | $2.08 | $+16.21 | $2,499.34 | ▲ $10,950.08 (+950.08) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 121 | $16.60 | $2.39 | $-77.34 | $4,505.55 | ▲ $10,947.69 (+947.69) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 8 | $217.03 | $2.04 | $+1.79 | $6,239.75 | ▲ $10,945.65 (+945.65) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 133 | $13.26 | $2.43 | $+278.48 | $8,000.90 | ▲ $10,943.22 (+943.22) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 482 | $2.36 | $6.31 | $-65.55 | $9,132.12 | ▲ $10,936.92 (+936.92) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 462 | $1.87 | $6.05 | $-39.73 | $9,990.01 | ▲ $10,930.87 (+930.87) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 9 | $58.79 | $2.04 | $-12.42 | $10,517.08 | ▲ $10,928.83 (+928.83) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 225 | $1.83 | $2.95 | $+108.90 | $10,925.88 | ▲ $10,925.88 (+925.88) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 101 | $24.00 | $2.29 | — | $8,499.59 | ▲ $10,923.59 (+923.59) | rank-weighted leftover; list flatten; ⚪; ret5=+13.0; leftover $2427.97 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 194 | $10.92 | $2.57 | — | $6,378.54 | ▲ $10,921.02 (+921.02) | rank-weighted leftover; list flatten; 🔵; ret5=+10.4; leftover $2124.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 29 | $61.47 | $2.08 | — | $4,593.83 | ▲ $10,918.94 (+918.94) | rank-weighted leftover; list flatten; 🔵; ret5=+9.2; leftover $1820.98 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 183 | $8.28 | $2.54 | — | $3,076.05 | ▲ $10,916.40 (+916.40) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1517.48 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 232 | $5.23 | $2.99 | — | $1,859.70 | ▲ $10,913.41 (+913.41) | rank-weighted leftover; list flatten; ret5=+10.7; leftover $1213.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $999.22 | ▲ $10,911.41 (+911.41) | rank-weighted leftover; list flatten; ret5=+6.1; leftover $910.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 374 | $1.62 | $4.82 | — | $388.52 | ▲ $10,906.59 (+906.59) | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $606.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 151 | $2.00 | $2.44 | — | $84.08 | ▲ $10,904.15 (+904.15) | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $303.50 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 194 | $10.79 | $2.62 | $-30.41 | $2,174.72 | ▲ $10,962.64 (+962.64) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 29 | $60.07 | $2.10 | $-44.78 | $3,914.64 | ▲ $10,960.53 (+960.53) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 183 | $8.60 | $2.58 | $+53.44 | $5,485.86 | ▲ $10,957.95 (+957.95) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 232 | $5.01 | $3.04 | $-57.07 | $6,645.14 | ▲ $10,954.91 (+954.91) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 2 | $427.50 | $2.02 | $-7.49 | $7,498.12 | ▲ $10,952.89 (+952.89) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 374 | $1.75 | $4.90 | $+38.90 | $8,147.73 | ▲ $10,948.00 (+948.00) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 151 | $1.93 | $2.48 | $-15.49 | $8,436.68 | ▲ $10,945.52 (+945.52) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 51 | $40.72 | $2.14 | — | $6,357.82 | ▲ $10,943.38 (+943.38) | rank-weighted leftover; list flatten; ret5=+1.8; leftover $2109.17 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 128 | $14.09 | $2.37 | — | $4,551.92 | ▲ $10,941.00 (+941.00) | rank-weighted leftover; list flatten; ret5=+1.1; leftover $1807.86 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 581 | $2.59 | $7.49 | — | $3,039.64 | ▲ $10,933.51 (+933.51) | rank-weighted leftover; list flatten; ret5=+4.2; leftover $1506.55 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 14 | $80.97 | $2.03 | — | $1,904.03 | ▲ $10,931.48 (+931.48) | rank-weighted leftover; list mover_buy; 🔵; ret5=-1.3; leftover $1205.24 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 204 | $4.42 | $2.63 | — | $999.71 | ▲ $10,928.84 (+928.84) | rank-weighted leftover; list mover_buy; 🔵; ret5=-8.6; leftover $903.93 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 8 | $75.12 | $2.01 | — | $396.74 | ▲ $10,926.83 (+926.83) | rank-weighted leftover; list mover_buy; 🔵; ret5=-2.2; leftover $602.62 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 14 | $81.65 | $2.05 | $+5.44 | $1,537.79 | ▲ $10,960.19 (+960.19) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 204 | $4.57 | $2.68 | $+25.29 | $2,467.39 | ▲ $10,957.51 (+957.51) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 8 | $74.54 | $2.03 | $-8.69 | $3,061.68 | ▲ $10,955.48 (+955.48) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $1,902.07 | ▲ $10,953.47 (+953.47) | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1224.67 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 54 | $16.95 | $2.15 | — | $984.61 | ▲ $10,951.31 (+951.31) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $918.50 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 33 | $18.50 | $2.09 | — | $372.02 | ▲ $10,949.22 (+949.22) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $612.34 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 33 | $9.19 | $2.09 | — | $66.67 | ▲ $10,947.14 (+947.14) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $306.17 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 101 | $23.75 | $2.33 | $-29.87 | $2,463.09 | ▲ $10,806.70 (+806.70) | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 51 | $41.11 | $2.17 | $+15.58 | $4,557.53 | ▲ $10,804.53 (+804.53) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 128 | $14.56 | $2.41 | $+55.38 | $6,418.80 | ▲ $10,802.12 (+802.12) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 581 | $2.51 | $7.60 | $-61.58 | $7,869.50 | ▲ $10,794.51 (+794.51) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.67 | $2.03 | $+27.71 | $9,056.83 | ▲ $10,792.48 (+792.48) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 54 | $15.44 | $2.17 | $-85.86 | $9,888.42 | ▲ $10,790.31 (+790.31) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 33 | $17.89 | $2.11 | $-24.33 | $10,476.68 | ▲ $10,788.20 (+788.20) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 33 | $9.44 | $2.11 | $+4.05 | $10,786.09 | ▲ $10,786.09 (+786.09) | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 48 | $49.76 | $2.13 | — | $8,395.48 | ▲ $10,783.96 (+783.96) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2396.91 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 50 | $41.31 | $2.14 | — | $6,327.84 | ▲ $10,781.82 (+781.82) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $2097.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 549 | $3.27 | $7.08 | — | $4,525.52 | ▲ $10,774.73 (+774.73) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1797.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 194 | $7.70 | $2.57 | — | $3,029.15 | ▲ $10,772.16 (+772.16) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1498.07 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $1,893.67 | ▲ $10,770.14 (+770.14) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1198.45 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 736 | $1.22 | $9.49 | — | $986.26 | ▲ $10,760.65 (+760.65) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $898.84 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 32 | $18.40 | $2.09 | — | $395.37 | ▲ $10,758.56 (+758.56) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $599.23 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 19 | $15.70 | $2.05 | — | $95.03 | ▲ $10,756.52 (+756.52) | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $299.61 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 50 | $42.93 | $2.17 | $+76.69 | $2,239.36 | ▲ $11,708.61 (+1,708.61) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 194 | $8.03 | $2.62 | $+58.83 | $3,794.56 | ▲ $11,705.99 (+1,705.99) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $4,984.58 | ▲ $11,703.96 (+1,703.96) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 32 | $18.27 | $2.11 | $-8.35 | $5,567.11 | ▲ $11,701.85 (+1,701.85) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 19 | $15.45 | $2.07 | $-8.86 | $5,858.59 | ▲ $11,699.78 (+1,699.78) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 7 | $266.94 | $2.01 | — | $3,988.00 | ▲ $11,697.77 (+1,697.77) | rank-weighted leftover; list flatten; ret5=+1.9; leftover $1952.86 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 50 | $30.65 | $2.14 | — | $2,453.36 | ▲ $11,695.63 (+1,695.63) | rank-weighted leftover; list flatten; 🔵; ret5=-2.2; leftover $1562.29 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 112 | $10.41 | $2.33 | — | $1,285.12 | ▲ $11,693.31 (+1,693.31) | rank-weighted leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1171.72 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 53 | $14.50 | $2.15 | — | $514.47 | ▲ $11,691.16 (+1,691.16) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $781.15 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 200 | $1.95 | $2.59 | — | $121.88 | ▲ $11,688.57 (+1,688.57) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $390.57 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-27 | `MU` | cash | leftover split 301.31 < 1 share @ 925.74 |
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
| `ATRC` | 48 | 2026-09-03 @ $49.76 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2396.91 |
| `CABA` | 549 | 2026-09-03 @ $3.27 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1797.68 |
| `GPRO` | 736 | 2026-09-03 @ $1.22 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $898.84 |
| `ASND` | 7 | 2026-09-04 @ $266.94 | rank-weighted leftover; list flatten; ret5=+1.9; leftover $1952.86 |
| `OSCR` | 50 | 2026-09-04 @ $30.65 | rank-weighted leftover; list flatten; 🔵; ret5=-2.2; leftover $1562.29 |
| `NVAX` | 112 | 2026-09-04 @ $10.41 | rank-weighted leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1171.72 |
| `BVS` | 53 | 2026-09-04 @ $14.50 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $781.15 |
| `BAK` | 200 | 2026-09-04 @ $1.95 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $390.57 |
