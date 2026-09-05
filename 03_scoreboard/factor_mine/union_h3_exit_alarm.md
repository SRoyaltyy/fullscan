# Factor mine action — `union_h3_exit_alarm`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · hold 3d, sell next 09:30 if 🚨

Cash book **+8.38%** ($10,838) · signal-only (no cash/fees) was +26.70%. Starts YES **16/17**. Fills 105 · skips 154 · realized $+571.64.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $61.98.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $97.53 | $10,055.59 | $10,153.12 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | BUY BTSG x20 @ 59.80; BUY IREN x27 @ 45.98; BUY TPG x24 @ 50.62; BUY TGTX x25 @ 49.70; BUY SLS x106 @ 11.70; BUY HIMS x42 @ 29.74; BUY INO x1543 @ 0.81; BUY TNDM x53 @ 23.33 |
| 2026-08-14 | +5.50 | $97.53 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53 | MARA, LDI, BTBT | — | $63.95 | $10,371.47 | $10,435.42 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | BUY MARA x1 @ 9.01; BUY LDI x13 @ 0.94; BUY BTBT x8 @ 1.50 |
| 2026-08-17 | +2.25 | $63.95 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, INO×1543, TNDM×53, MARA×1, LDI×13, BTBT×8 | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | INO | $127.33 | $10,237.30 | $10,364.63 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | SELL INO (exit 🚨 after 2 sess); BUY DVN x4 @ 46.18; BUY EOG x1 @ 142.77; BUY FANG x1 @ 202.70; BUY TMC x52 @ 4.05; BUY TGB x25 @ 8.46; BUY ELF x2 @ 90.54; BUY DNN x65 @ 3.24; BUY NB x41 @ 5.07 |
| 2026-08-18 | -6.20 | $127.33 | BTSG×20, IREN×27, TPG×24, TGTX×25, SLS×106, HIMS×42, TNDM×53, MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, TNDM | $8,649.10 | $1,570.09 | $10,219.20 | MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | SELL BTSG (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL TGTX (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL TNDM (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $8,649.10 | MARA×1, LDI×13, BTBT×8, DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | — | MARA, LDI, BTBT | $8,680.37 | $1,560.44 | $10,240.81 | DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | SELL MARA (dropped from list after 3 sess (min 3)); SELL LDI (dropped from list after 3 sess (min 3)); SELL BTBT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $8,680.37 | DVN×4, EOG×1, FANG×1, TMC×52, TGB×25, ELF×2, DNN×65, NB×41 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, EOG, FANG, TMC, TGB, ELF, DNN, NB | $132.91 | $10,306.77 | $10,439.68 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, WPM×8 | SELL DVN (dropped from list after 3 sess (min 3)); SELL EOG (dropped from list after 3 sess (min 3)); SELL FANG (dropped from list after 3 sess (min 3)); SELL TMC (dropped from list after 3 sess (min 3)); SELL TGB (dropped from list after 3 sess (min 3)); SELL ELF (dropped from list after 3 sess (min 3)); SELL DNN (dropped from list after 3 sess (min 3)); SELL NB (dropped from list after 3 sess (min 3)); BUY AG x62 @ 20.55; BUY BHP x14 @ 91.01; BUY CDE x61 @ 20.65; BUY HDSN x221 @ 5.77; BUY IAG x65 @ 19.63; BUY KGC x43 @ 29.63; BUY NFGC x730 @ 1.75; BUY WPM x8 @ 144.54 |
| 2026-08-21 | +3.25 | $132.91 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, WPM×8 | ARCT, AUTL, CRDL, CYPH | — | $75.02 | $10,637.45 | $10,712.47 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, WPM×8, ARCT×1, AUTL×6, CRDL×8, CYPH×12 | BUY ARCT x1 @ 11.13; BUY AUTL x6 @ 2.47; BUY CRDL x8 @ 1.93; BUY CYPH x12 @ 1.32 |
| 2026-08-24 | -5.17 | $75.02 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, WPM×8, ARCT×1, AUTL×6, CRDL×8, CYPH×12 | — | WPM, ARCT | $1,357.77 | $9,329.41 | $10,687.18 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, AUTL×6, CRDL×8, CYPH×12 | SELL WPM (exit 🚨 after 2 sess); SELL ARCT (exit 🚨 after 1 sess); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $1,357.77 | AG×62, BHP×14, CDE×61, HDSN×221, IAG×65, KGC×43, NFGC×730, AUTL×6, CRDL×8, CYPH×12 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC | $76.53 | $10,609.26 | $10,685.79 | AUTL×6, CRDL×8, CYPH×12, MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); BUY MOS x55 @ 24.00; BUY OCUL x122 @ 10.92; BUY INSP x21 @ 61.47; BUY CRMD x161 @ 8.28; BUY RZLT x254 @ 5.23; BUY HCA x3 @ 429.24; BUY BMEA x822 @ 1.62; BUY NPWR x666 @ 2.00 |
| 2026-08-26 | +2.02 | $76.53 | AUTL×6, CRDL×8, CYPH×12, MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666 | — | — | $76.53 | $10,605.49 | $10,682.02 | AUTL×6, CRDL×8, CYPH×12, MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666 | hold AUTL,CRDL,CYPH,MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,NPWR |
| 2026-08-27 | — | $76.53 | AUTL×6, CRDL×8, CYPH×12, MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666 | CRK, SLI, GGB | AUTL, CRDL, CYPH | $77.98 | $10,512.05 | $10,590.03 | MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666, CRK×1, SLI×6, GGB×4 | SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); BUY CRK x1 @ 14.09; BUY SLI x6 @ 2.59; BUY GGB x4 @ 4.42 |
| 2026-08-28 | +0.75 | $77.98 | MOS×55, OCUL×122, INSP×21, CRMD×161, RZLT×254, HCA×3, BMEA×822, NPWR×666, CRK×1, SLI×6, GGB×4 | RRC, ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $153.51 | $10,472.18 | $10,625.69 | MOS×55, CRK×1, SLI×6, GGB×4, RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | SELL OCUL (dropped from list after 3 sess (min 3)); SELL INSP (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL RZLT (dropped from list after 3 sess (min 3)); SELL HCA (dropped from list after 3 sess (min 3)); SELL BMEA (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); BUY RRC x44 @ 41.44; BUY ANF x12 @ 144.70; BUY BHVN x108 @ 16.95; BUY BZ x99 @ 18.50; BUY CAPR x200 @ 9.19 |
| 2026-08-31 | -5.85 | $153.51 | MOS×55, CRK×1, SLI×6, GGB×4, RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | — | MOS | $1,457.59 | $8,984.75 | $10,442.34 | CRK×1, SLI×6, GGB×4, RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | SELL MOS (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $1,457.59 | CRK×1, SLI×6, GGB×4, RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | — | CRK, SLI, GGB | $1,505.96 | $8,940.51 | $10,446.47 | RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | SELL CRK (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL GGB (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $1,505.96 | RRC×44, ANF×12, BHVN×108, BZ×99, CAPR×200 | — | RRC, ANF, BHVN, BZ, CAPR | $10,571.65 | $0.00 | $10,571.65 | — | SELL RRC (dropped from list after 3 sess (min 3)); SELL ANF (dropped from list after 3 sess (min 3)); SELL BHVN (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,571.65 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $121.34 | $11,231.31 | $11,352.65 | ATRC×26, HRMY×31, CABA×404, VSTM×171, RVTY×10, GPRO×1083, FRVO×71, CRK×84 | BUY ATRC x26 @ 49.76; BUY HRMY x31 @ 41.31; BUY CABA x404 @ 3.27; BUY VSTM x171 @ 7.70; BUY RVTY x10 @ 125.94; BUY GPRO x1083 @ 1.22; BUY FRVO x71 @ 18.40; BUY CRK x84 @ 15.70 |
| 2026-09-04 | — | $121.34 | ATRC×26, HRMY×31, CABA×404, VSTM×171, RVTY×10, GPRO×1083, FRVO×71, CRK×84 | NVAX, BVS, BAK | — | $61.98 | $10,776.27 | $10,838.25 | ATRC×26, HRMY×31, CABA×404, VSTM×171, RVTY×10, GPRO×1083, FRVO×71, CRK×84, NVAX×2, BVS×1, BAK×12 | BUY NVAX x2 @ 10.41; BUY BVS x1 @ 14.50; BUY BAK x12 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 20 | $59.80 | $2.05 | — | $8,801.95 | ▼ $9,997.95 (-2.05) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 27 | $45.98 | $2.07 | — | $7,558.42 | ▼ $9,995.88 (-4.12) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+12.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 24 | $50.62 | $2.06 | — | $6,341.40 | ▼ $9,993.82 (-6.18) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+6.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 25 | $49.70 | $2.06 | — | $5,096.84 | ▼ $9,991.75 (-8.25) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 106 | $11.70 | $2.31 | — | $3,854.33 | ▼ $9,989.44 (-10.56) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-0.8; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 42 | $29.74 | $2.12 | — | $2,603.13 | ▼ $9,987.33 (-12.67) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=-5.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1543 | $0.81 | $17.13 | — | $1,336.17 | ▼ $9,970.20 (-29.80) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 53 | $23.33 | $2.15 | — | $97.53 | ▼ $9,968.05 (-31.95) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+19.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $88.43 | ▲ $10,178.03 (+178.03) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-13.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 13 | $0.94 | $0.16 | — | $76.09 | ▲ $10,177.87 (+177.87) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.5; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 8 | $1.50 | $0.14 | — | $63.95 | ▲ $10,177.73 (+177.73) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+9.2; leftover $12.19 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `INO` | 1543 | $1.07 | $20.17 | $+363.88 | $1,694.78 | ▲ $10,394.60 (+394.60) | exit 🚨 after 2 sess | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 4 | $46.18 | $1.86 | — | $1,508.20 | ▲ $10,392.74 (+392.74) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+6.7; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 1 | $142.77 | $1.43 | — | $1,364.00 | ▲ $10,391.31 (+391.31) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+5.8; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 1 | $202.70 | $1.99 | — | $1,159.31 | ▲ $10,389.32 (+389.32) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+8.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 52 | $4.05 | $2.15 | — | $946.56 | ▲ $10,387.17 (+387.17) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=-12.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 25 | $8.46 | $2.06 | — | $733.00 | ▲ $10,385.11 (+385.11) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.4; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ELF` | 2 | $90.54 | $1.82 | — | $550.10 | ▲ $10,383.29 (+383.29) | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=-7.2; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 65 | $3.24 | $2.19 | — | $337.32 | ▲ $10,381.11 (+381.11) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+0.3; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 41 | $5.07 | $2.11 | — | $127.33 | ▲ $10,378.99 (+378.99) | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=-4.7; leftover $211.85 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 20 | $60.00 | $2.07 | $-0.12 | $1,325.26 | ▲ $10,231.24 (+231.24) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 27 | $43.56 | $2.09 | $-69.50 | $2,499.29 | ▲ $10,229.15 (+229.15) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 24 | $51.77 | $2.08 | $+23.38 | $3,739.69 | ▲ $10,227.07 (+227.07) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 25 | $49.28 | $2.08 | $-14.65 | $4,969.61 | ▲ $10,224.99 (+224.99) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 106 | $12.66 | $2.34 | $+97.12 | $6,309.23 | ▲ $10,222.65 (+222.65) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 42 | $27.85 | $2.14 | $-83.63 | $7,476.79 | ▲ $10,220.51 (+220.51) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 53 | $22.16 | $2.17 | $-66.33 | $8,649.10 | ▲ $10,218.34 (+218.34) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $8,657.90 | ▲ $10,244.09 (+244.09) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 13 | $0.88 | $0.17 | $-1.08 | $8,669.17 | ▲ $10,243.92 (+243.92) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 8 | $1.42 | $0.16 | $-0.94 | $8,680.37 | ▲ $10,243.76 (+243.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 4 | $49.02 | $1.99 | $+7.51 | $8,874.46 | ▲ $10,238.76 (+238.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `EOG` | 1 | $151.45 | $1.54 | $+5.71 | $9,024.37 | ▲ $10,237.22 (+237.22) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FANG` | 1 | $213.51 | $2.01 | $+6.80 | $9,235.87 | ▲ $10,235.21 (+235.21) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 52 | $3.92 | $2.17 | $-11.07 | $9,437.54 | ▲ $10,233.04 (+233.04) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 25 | $8.35 | $2.08 | $-6.90 | $9,644.21 | ▲ $10,230.96 (+230.96) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ELF` | 2 | $98.15 | $1.99 | $+11.41 | $9,838.52 | ▲ $10,228.97 (+228.97) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 65 | $3.20 | $2.21 | $-6.99 | $10,044.31 | ▲ $10,226.76 (+226.76) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 41 | $4.45 | $1.97 | $-29.50 | $10,224.79 | ▲ $10,224.79 (+224.79) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 62 | $20.55 | $2.18 | — | $8,948.52 | ▲ $10,222.62 (+222.62) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $7,672.35 | ▲ $10,220.59 (+220.59) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $6,410.52 | ▲ $10,218.41 (+218.41) | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 221 | $5.77 | $2.85 | — | $5,132.50 | ▲ $10,215.56 (+215.56) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 65 | $19.63 | $2.19 | — | $3,854.37 | ▲ $10,213.38 (+213.38) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 43 | $29.63 | $2.12 | — | $2,578.16 | ▲ $10,211.26 (+211.26) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 730 | $1.75 | $9.42 | — | $1,291.24 | ▲ $10,201.84 (+201.84) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $132.91 | ▲ $10,199.83 (+199.83) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1278.10 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $121.66 | ▲ $10,714.15 (+714.15) | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $16.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 6 | $2.47 | $0.17 | — | $106.68 | ▲ $10,713.99 (+713.99) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $16.61 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 8 | $1.93 | $0.18 | — | $91.06 | ▲ $10,713.81 (+713.81) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $16.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 12 | $1.32 | $0.19 | — | $75.02 | ▲ $10,713.61 (+713.61) | hold 3d, sell next 09:30 if 🚨; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $16.61 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `WPM` | 8 | $158.96 | $2.03 | $+111.31 | $1,344.67 | ▲ $10,836.69 (+836.69) | exit 🚨 after 2 sess | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 1 | $13.26 | $0.16 | $+1.86 | $1,357.77 | ▲ $10,836.53 (+836.53) | exit 🚨 after 1 sess | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 62 | $20.73 | $2.20 | $+6.79 | $2,640.84 | ▲ $10,736.57 (+736.57) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 14 | $95.95 | $2.05 | $+65.08 | $3,982.09 | ▲ $10,734.52 (+734.52) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 61 | $20.85 | $2.19 | $+7.83 | $5,251.74 | ▲ $10,732.32 (+732.32) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 221 | $5.53 | $2.90 | $-58.79 | $6,470.97 | ▲ $10,729.42 (+729.42) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 65 | $21.63 | $2.21 | $+125.61 | $7,874.72 | ▲ $10,727.22 (+727.22) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 43 | $32.76 | $2.14 | $+130.33 | $9,281.26 | ▲ $10,725.08 (+725.08) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 730 | $1.91 | $9.55 | $+97.83 | $10,666.01 | ▲ $10,715.53 (+715.53) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $9,343.85 | ▲ $10,713.37 (+713.37) | hold 3d, sell next 09:30 if 🚨; list flatten; ⚪; ret5=+13.0; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 122 | $10.92 | $2.36 | — | $8,009.26 | ▲ $10,711.02 (+711.02) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+10.4; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $6,716.33 | ▲ $10,708.96 (+708.96) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ret5=+9.2; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 161 | $8.28 | $2.47 | — | $5,380.78 | ▲ $10,706.49 (+706.49) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 254 | $5.23 | $3.28 | — | $4,049.08 | ▲ $10,703.21 (+703.21) | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+10.7; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $2,759.36 | ▲ $10,701.21 (+701.21) | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+6.1; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 822 | $1.62 | $10.60 | — | $1,417.12 | ▲ $10,690.61 (+690.61) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1333.25 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 666 | $2.00 | $8.59 | — | $76.53 | ▲ $10,682.02 (+682.02) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1333.25 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 6 | $2.41 | $0.18 | $-0.71 | $90.81 | ▲ $10,733.82 (+733.82) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 8 | $2.03 | $0.21 | $+0.42 | $106.84 | ▲ $10,733.61 (+733.61) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 12 | $1.60 | $0.25 | $+2.92 | $125.79 | ▲ $10,733.36 (+733.36) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 1 | $14.09 | $0.14 | — | $111.56 | ▲ $10,733.22 (+733.22) | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+1.1; leftover $17.97 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 6 | $2.59 | $0.17 | — | $95.85 | ▲ $10,733.05 (+733.05) | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+4.2; leftover $17.97 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 4 | $4.42 | $0.19 | — | $77.98 | ▲ $10,732.86 (+732.86) | hold 3d, sell next 09:30 if 🚨; list mover_buy; 🔵; ret5=-8.6; leftover $17.97 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 122 | $10.63 | $2.39 | $-40.12 | $1,372.45 | ▲ $10,622.41 (+622.41) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 21 | $62.10 | $2.07 | $+9.10 | $2,674.48 | ▲ $10,620.34 (+620.34) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 161 | $8.49 | $2.51 | $+28.83 | $4,038.86 | ▲ $10,617.83 (+617.83) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 254 | $5.07 | $3.33 | $-47.25 | $5,323.31 | ▲ $10,614.50 (+614.50) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $6,595.12 | ▲ $10,612.48 (+612.48) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 822 | $1.74 | $10.75 | $+77.28 | $8,014.65 | ▲ $10,601.73 (+601.73) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 666 | $1.83 | $8.71 | $-130.52 | $9,224.71 | ▲ $10,593.01 (+593.01) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 44 | $41.44 | $2.12 | — | $7,399.23 | ▲ $10,590.89 (+590.89) | hold 3d, sell next 09:30 if 🚨; list flatten; ret5=+1.8; leftover $1844.94 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 12 | $144.70 | $2.03 | — | $5,660.81 | ▲ $10,588.87 (+588.87) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1844.94 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 108 | $16.95 | $2.31 | — | $3,827.89 | ▲ $10,586.55 (+586.55) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1844.94 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 99 | $18.50 | $2.29 | — | $1,994.10 | ▲ $10,584.26 (+584.26) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1844.94 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 200 | $9.19 | $2.59 | — | $153.51 | ▲ $10,581.67 (+581.67) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1844.94 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.75 | $2.18 | $-18.08 | $1,457.59 | ▲ $10,424.92 (+424.92) | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 1 | $14.31 | $0.17 | $-0.09 | $1,471.73 | ▲ $10,508.32 (+508.32) | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 6 | $2.70 | $0.20 | $+0.29 | $1,487.73 | ▲ $10,508.12 (+508.12) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 4 | $4.61 | $0.22 | $+0.35 | $1,505.96 | ▲ $10,507.91 (+507.91) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 44 | $41.94 | $2.15 | $+17.73 | $3,349.17 | ▲ $10,581.00 (+581.00) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 12 | $142.00 | $2.05 | $-36.48 | $5,051.12 | ▲ $10,578.95 (+578.95) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 108 | $15.39 | $2.35 | $-173.14 | $6,710.90 | ▲ $10,576.61 (+576.61) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 99 | $17.29 | $2.32 | $-124.39 | $8,420.29 | ▲ $10,574.29 (+574.29) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 200 | $10.77 | $2.64 | $+310.77 | $10,571.65 | ▲ $10,571.65 (+571.65) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $9,275.82 | ▲ $10,569.58 (+569.58) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 31 | $41.31 | $2.08 | — | $7,993.13 | ▲ $10,567.50 (+567.50) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 404 | $3.27 | $5.21 | — | $6,666.84 | ▲ $10,562.29 (+562.29) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 171 | $7.70 | $2.50 | — | $5,347.63 | ▲ $10,559.78 (+559.78) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,086.21 | ▲ $10,557.76 (+557.76) | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1321.46 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1083 | $1.22 | $13.97 | — | $2,750.98 | ▲ $10,543.79 (+543.79) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1321.46 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 71 | $18.40 | $2.20 | — | $1,442.38 | ▲ $10,541.59 (+541.59) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1321.46 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 84 | $15.70 | $2.24 | — | $121.34 | ▲ $10,539.35 (+539.35) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1321.46 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $100.30 | ▲ $11,513.69 (+1,513.69) | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $24.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $85.65 | ▲ $11,513.54 (+1,513.54) | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.8; leftover $24.27 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 12 | $1.95 | $0.27 | — | $61.98 | ▲ $11,513.27 (+1,513.27) | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $24.27 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 12.19 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 12.19 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 12.19 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 12.19 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 12.19 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `EOG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FANG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ELF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `EOG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FANG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ELF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 16.61 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 16.61 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 16.61 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 16.61 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 17.97 < 1 share @ 40.72 |
| 2026-08-27 | `ACMR` | cash | leftover split 17.97 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 17.97 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 17.97 < 1 share @ 925.74 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 24.27 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 24.27 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 26 | 2026-09-03 @ $49.76 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1321.46 |
| `HRMY` | 31 | 2026-09-03 @ $41.31 | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1321.46 |
| `CABA` | 404 | 2026-09-03 @ $3.27 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1321.46 |
| `VSTM` | 171 | 2026-09-03 @ $7.70 | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1321.46 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | hold 3d, sell next 09:30 if 🚨; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1321.46 |
| `GPRO` | 1083 | 2026-09-03 @ $1.22 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1321.46 |
| `FRVO` | 71 | 2026-09-03 @ $18.40 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1321.46 |
| `CRK` | 84 | 2026-09-03 @ $15.70 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1321.46 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | hold 3d, sell next 09:30 if 🚨; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $24.27 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | hold 3d, sell next 09:30 if 🚨; list flatten; 🔵; ⚪; ret5=+0.8; leftover $24.27 |
| `BAK` | 12 | 2026-09-04 @ $1.95 | hold 3d, sell next 09:30 if 🚨; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $24.27 |
