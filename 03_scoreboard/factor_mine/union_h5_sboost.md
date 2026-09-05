# Factor mine action — `union_h5_sboost`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `both` · S≥+5: sizeup + more names

Cash book **+18.18%** ($11,818) · signal-only (no cash/fees) was +58.01%. Starts YES **14/17**. Fills 108 · skips 259 · realized $+1479.28.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8 (S≥+5 may raise this when S-boost is `both`).
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $213.05.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | — | $123.82 | $10,071.92 | $10,195.74 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | S=+8.53 more_names top_n=12; S=+8.53 sizeup x1.35; BUY BTSG x18 @ 59.80; BUY IREN x24 @ 45.98; BUY TPG x21 @ 50.62; BUY TGTX x22 @ 49.70; BUY SLS x94 @ 11.70; BUY HIMS x37 @ 29.74; BUY INO x1371 @ 0.81; BUY TNDM x47 @ 23.33; BUY VOR x50 @ 22.01 |
| 2026-08-14 | +5.50 | $123.82 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50 | MARA, LDI, BTBT, ANGX, HYLN | — | $78.00 | $10,356.08 | $10,434.08 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2 | S=+5.50 more_names top_n=12; S=+5.50 sizeup x1.35; BUY MARA x1 @ 9.01; BUY LDI x11 @ 0.94; BUY BTBT x6 @ 1.50; BUY ANGX x2 @ 4.31; BUY HYLN x2 @ 4.18 |
| 2026-08-17 | +2.25 | $78.00 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2 | TMC, TGB, DNN, HNST | — | $41.72 | $10,471.30 | $10,513.02 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | BUY TMC x2 @ 4.05; BUY TGB x1 @ 8.46; BUY DNN x3 @ 3.24; BUY HNST x2 @ 4.81 |
| 2026-08-18 | -6.20 | $41.72 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | — | — | $41.72 | $10,526.15 | $10,567.86 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $41.72 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | — | — | $41.72 | $10,946.55 | $10,988.26 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $41.72 | BTSG×18, IREN×24, TPG×21, TGTX×22, SLS×94, HIMS×37, INO×1371, TNDM×47, VOR×50, MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM, VOR | $149.46 | $10,947.97 | $11,097.43 | MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9 | SELL BTSG (dropped from list after 5 sess (min 5)); SELL IREN (dropped from list after 5 sess (min 5)); SELL TPG (dropped from list after 5 sess (min 5)); SELL TGTX (dropped from list after 5 sess (min 5)); SELL SLS (dropped from list after 5 sess (min 5)); SELL HIMS (dropped from list after 5 sess (min 5)); SELL INO (dropped from list after 5 sess (min 5)); SELL TNDM (dropped from list after 5 sess (min 5)); SELL VOR (dropped from list after 5 sess (min 5)); BUY AG x65 @ 20.55; BUY BHP x14 @ 91.01; BUY CDE x65 @ 20.65; BUY HDSN x233 @ 5.77; BUY IAG x68 @ 19.63; BUY KGC x45 @ 29.63; BUY NFGC x770 @ 1.75; BUY WPM x9 @ 144.54 |
| 2026-08-21 | +3.25 | $149.46 | MARA×1, LDI×11, BTBT×6, ANGX×2, HYLN×2, TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9 | AUPH, ARCT, AUTL, CRDL, CYPH | MARA, LDI, BTBT, ANGX, HYLN | $85.90 | $11,305.14 | $11,391.04 | TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | SELL MARA (dropped from list after 5 sess (min 5)); SELL LDI (dropped from list after 5 sess (min 5)); SELL BTBT (dropped from list after 5 sess (min 5)); SELL ANGX (dropped from list after 5 sess (min 5)); SELL HYLN (dropped from list after 5 sess (min 5)); BUY AUPH x1 @ 17.20; BUY ARCT x2 @ 11.13; BUY AUTL x9 @ 2.47; BUY CRDL x12 @ 1.93; BUY CYPH x18 @ 1.32 |
| 2026-08-24 | -5.17 | $85.90 | TMC×2, TGB×1, DNN×3, HNST×2, AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | — | TMC, TGB, DNN, HNST | $124.40 | $11,235.24 | $11,359.64 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | SELL TMC (dropped from list after 5 sess (min 5)); SELL TGB (dropped from list after 5 sess (min 5)); SELL DNN (dropped from list after 5 sess (min 5)); SELL HNST (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $124.40 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18 | OCUL, CRMD, RZLT, BMEA, NPWR | — | $65.52 | $11,303.78 | $11,369.30 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7 | BUY OCUL x1 @ 10.92; BUY CRMD x1 @ 8.28; BUY RZLT x2 @ 5.23; BUY BMEA x9 @ 1.62; BUY NPWR x7 @ 2.00 |
| 2026-08-26 | +2.02 | $65.52 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7 | — | — | $65.52 | $11,368.14 | $11,433.66 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7 | hold AG,BHP,CDE,HDSN,IAG,KGC,NFGC,WPM,AUPH,ARCT,AUTL,CRDL,CYPH,OCUL,CRMD,RZLT,BMEA,NPWR |
| 2026-08-27 | — | $65.52 | AG×65, BHP×14, CDE×65, HDSN×233, IAG×68, KGC×45, NFGC×770, WPM×9, AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7 | RRC, CRK, MOS, SLI, ACMR, GGB, MT, MU | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $611.47 | $10,898.38 | $11,509.85 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1 | SELL AG (dropped from list after 5 sess (min 5)); SELL BHP (dropped from list after 5 sess (min 5)); SELL CDE (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL WPM (dropped from list after 5 sess (min 5)); BUY RRC x34 @ 40.72; BUY CRK x100 @ 14.09; BUY MOS x57 @ 24.84; BUY SLI x546 @ 2.59; BUY ACMR x17 @ 80.97; BUY GGB x320 @ 4.42; BUY MT x18 @ 75.12; BUY MU x1 @ 925.74 |
| 2026-08-28 | +0.75 | $611.47 | AUPH×1, ARCT×2, AUTL×9, CRDL×12, CYPH×18, OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1 | ANF, BHVN, BZ, CAPR | AUPH, ARCT, AUTL, CRDL, CYPH | $64.14 | $11,552.17 | $11,616.31 | OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | SELL AUPH (dropped from list after 5 sess (min 5)); SELL ARCT (dropped from list after 5 sess (min 5)); SELL AUTL (dropped from list after 5 sess (min 5)); SELL CRDL (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)); BUY ANF x1 @ 144.70; BUY BHVN x10 @ 16.95; BUY BZ x9 @ 18.50; BUY CAPR x20 @ 9.19 |
| 2026-08-31 | -5.85 | $64.14 | OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | — | — | $64.14 | $11,320.48 | $11,384.62 | OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $64.14 | OCUL×1, CRMD×1, RZLT×2, BMEA×9, NPWR×7, RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | — | OCUL, CRMD, RZLT, BMEA, NPWR | $118.86 | $11,362.82 | $11,481.68 | RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | SELL OCUL (dropped from list after 5 sess (min 5)); SELL CRMD (dropped from list after 5 sess (min 5)); SELL RZLT (dropped from list after 5 sess (min 5)); SELL BMEA (dropped from list after 5 sess (min 5)); SELL NPWR (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $118.86 | RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | — | — | $118.86 | $11,416.33 | $11,535.19 | RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $118.86 | RRC×34, CRK×100, MOS×57, SLI×546, ACMR×17, GGB×320, MT×18, MU×1, ANF×1, BHVN×10, BZ×9, CAPR×20 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO | RRC, MOS, SLI, ACMR, GGB, MT, MU | $84.86 | $12,211.40 | $12,296.26 | CRK×100, ANF×1, BHVN×10, BZ×9, CAPR×20, ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1088, FRVO×72 | SELL RRC (dropped from list after 5 sess (min 5)); SELL MOS (dropped from list after 5 sess (min 5)); SELL SLI (dropped from list after 5 sess (min 5)); SELL ACMR (dropped from list after 5 sess (min 5)); SELL GGB (dropped from list after 5 sess (min 5)); SELL MT (dropped from list after 5 sess (min 5)); SELL MU (dropped from list after 5 sess (min 5)); BUY ATRC x26 @ 49.76; BUY HRMY x32 @ 41.31; BUY CABA x406 @ 3.27; BUY VSTM x172 @ 7.70; BUY RVTY x10 @ 125.94; BUY GPRO x1088 @ 1.22; BUY FRVO x72 @ 18.40 |
| 2026-09-04 | — | $84.86 | CRK×100, ANF×1, BHVN×10, BZ×9, CAPR×20, ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1088, FRVO×72 | ASND, OSCR, NVAX, BVS, BAK | CRK, ANF, BHVN, BZ, CAPR | $213.05 | $11,605.40 | $11,818.45 | ATRC×26, HRMY×32, CABA×406, VSTM×172, RVTY×10, GPRO×1088, FRVO×72, ASND×1, OSCR×14, NVAX×43, BVS×31, BAK×232 | SELL CRK (dropped from list after 6 sess (min 5)); SELL ANF (dropped from list after 5 sess (min 5)); SELL BHVN (dropped from list after 5 sess (min 5)); SELL BZ (dropped from list after 5 sess (min 5)); SELL CAPR (dropped from list after 5 sess (min 5)); BUY ASND x1 @ 266.94; BUY OSCR x14 @ 30.65; BUY NVAX x43 @ 10.41; BUY BVS x31 @ 14.50; BUY BAK x232 @ 1.95 |

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
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $114.71 | ▲ $10,219.53 (+219.53) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-13.5; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 11 | $0.94 | $0.14 | — | $104.27 | ▲ $10,219.40 (+219.40) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.5; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 6 | $1.50 | $0.11 | — | $95.16 | ▲ $10,219.29 (+219.29) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+9.2; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $86.45 | ▲ $10,219.20 (+219.20) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 2 | $4.18 | $0.09 | — | $78.00 | ▲ $10,219.11 (+219.11) | S≥+5: sizeup + more names; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $10.32 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.81 | ▲ $10,410.39 (+410.39) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=-12.3; leftover $9.75 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.27 | ▲ $10,410.30 (+410.30) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.4; leftover $9.75 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.44 | ▲ $10,410.20 (+410.20) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=+0.3; leftover $9.75 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.72 | ▲ $10,410.09 (+410.09) | S≥+5: sizeup + more names; list flatten; ⚪; ret5=-11.4; leftover $9.75 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 18 | $58.64 | $2.06 | $-24.99 | $1,095.17 | ▲ $10,901.75 (+901.75) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 24 | $42.46 | $2.08 | $-88.62 | $2,112.13 | ▲ $10,899.67 (+899.67) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 21 | $53.06 | $2.07 | $+47.05 | $3,224.32 | ▲ $10,897.60 (+897.60) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 22 | $51.65 | $2.08 | $+38.77 | $4,358.54 | ▲ $10,895.52 (+895.52) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $5,657.20 | ▲ $10,893.22 (+893.22) | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 37 | $30.66 | $2.12 | $+29.82 | $6,789.50 | ▲ $10,891.10 (+891.10) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 1371 | $1.30 | $17.93 | $+638.64 | $8,553.88 | ▲ $10,873.18 (+873.18) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 47 | $23.11 | $2.15 | $-14.62 | $9,637.89 | ▲ $10,871.02 (+871.02) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `VOR` | 50 | $23.05 | $2.16 | $+47.70 | $10,788.23 | ▲ $10,868.86 (+868.86) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 65 | $20.55 | $2.19 | — | $9,450.30 | ▲ $10,866.68 (+866.68) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 14 | $91.01 | $2.03 | — | $8,174.13 | ▲ $10,864.65 (+864.65) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 65 | $20.65 | $2.19 | — | $6,829.69 | ▲ $10,862.46 (+862.46) | S≥+5: sizeup + more names; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 233 | $5.77 | $3.01 | — | $5,482.28 | ▲ $10,859.46 (+859.46) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 68 | $19.63 | $2.19 | — | $4,145.24 | ▲ $10,857.26 (+857.26) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 45 | $29.63 | $2.12 | — | $2,809.77 | ▲ $10,855.14 (+855.14) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 770 | $1.75 | $9.93 | — | $1,452.33 | ▲ $10,845.21 (+845.21) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $149.46 | ▲ $10,843.19 (+843.19) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1348.53 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $161.02 | ▲ $11,389.29 (+1,389.29) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 11 | $0.87 | $0.15 | $-1.05 | $170.41 | ▲ $11,389.15 (+1,389.15) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 6 | $1.66 | $0.14 | $+0.71 | $180.23 | ▲ $11,389.01 (+1,389.01) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 2 | $4.43 | $0.11 | $+0.03 | $188.97 | ▲ $11,388.89 (+1,388.89) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 2 | $3.42 | $0.09 | $-1.70 | $195.72 | ▲ $11,388.80 (+1,388.80) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $178.34 | ▲ $11,388.62 (+1,388.62) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 2 | $11.13 | $0.23 | — | $155.86 | ▲ $11,388.40 (+1,388.40) | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 9 | $2.47 | $0.25 | — | $133.38 | ▲ $11,388.15 (+1,388.15) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 12 | $1.93 | $0.27 | — | $109.95 | ▲ $11,387.88 (+1,387.88) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 18 | $1.32 | $0.29 | — | $85.90 | ▲ $11,387.59 (+1,387.59) | S≥+5: sizeup + more names; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $24.46 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 2 | $4.57 | $0.12 | $+0.84 | $94.92 | ▲ $11,525.71 (+1,525.71) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $104.06 | ▲ $11,525.59 (+1,525.59) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 3 | $3.50 | $0.13 | $+0.54 | $114.43 | ▲ $11,525.46 (+1,525.46) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `HNST` | 2 | $5.05 | $0.13 | $+0.25 | $124.40 | ▲ $11,525.33 (+1,525.33) | dropped from list after 5 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $113.37 | ▲ $11,434.19 (+1,434.19) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=+10.4; leftover $15.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.28 | $0.09 | — | $105.01 | ▲ $11,434.11 (+1,434.11) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+8.8; leftover $15.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 2 | $5.23 | $0.11 | — | $94.43 | ▲ $11,433.99 (+1,433.99) | S≥+5: sizeup + more names; list flatten; ret5=+10.7; leftover $15.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 9 | $1.62 | $0.17 | — | $79.68 | ▲ $11,433.82 (+1,433.82) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $15.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 7 | $2.00 | $0.16 | — | $65.52 | ▲ $11,433.66 (+1,433.66) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $15.55 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 65 | $20.63 | $2.21 | $+0.81 | $1,404.26 | ▲ $11,532.16 (+1,532.16) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 14 | $96.99 | $2.05 | $+79.64 | $2,760.07 | ▲ $11,530.11 (+1,530.11) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 65 | $21.00 | $2.21 | $+18.36 | $4,122.86 | ▲ $11,527.90 (+1,527.90) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 233 | $5.51 | $3.05 | $-66.64 | $5,403.64 | ▲ $11,524.85 (+1,524.85) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 68 | $21.64 | $2.22 | $+132.27 | $6,872.94 | ▲ $11,522.63 (+1,522.63) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 45 | $32.90 | $2.15 | $+142.88 | $8,351.30 | ▲ $11,520.49 (+1,520.49) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 770 | $2.00 | $10.07 | $+172.49 | $9,881.22 | ▲ $11,510.41 (+1,510.41) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $11,327.55 | ▲ $11,508.37 (+1,508.37) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 34 | $40.72 | $2.09 | — | $9,940.98 | ▲ $11,506.28 (+1,506.28) | S≥+5: sizeup + more names; list flatten; ret5=+1.8; leftover $1415.94 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 100 | $14.09 | $2.29 | — | $8,529.69 | ▲ $11,503.99 (+1,503.99) | S≥+5: sizeup + more names; list flatten; ret5=+1.1; leftover $1415.94 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 57 | $24.84 | $2.16 | — | $7,111.65 | ▲ $11,501.83 (+1,501.83) | S≥+5: sizeup + more names; list flatten; ret5=+13.0; leftover $1415.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 546 | $2.59 | $7.04 | — | $5,690.47 | ▲ $11,494.79 (+1,494.79) | S≥+5: sizeup + more names; list flatten; ret5=+4.2; leftover $1415.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $80.97 | $2.04 | — | $4,311.94 | ▲ $11,492.75 (+1,492.75) | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-1.3; leftover $1415.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 320 | $4.42 | $4.13 | — | $2,893.41 | ▲ $11,488.62 (+1,488.62) | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-8.6; leftover $1415.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $1,539.21 | ▲ $11,486.58 (+1,486.58) | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-2.2; leftover $1415.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $611.47 | ▲ $11,484.58 (+1,484.58) | S≥+5: sizeup + more names; list mover_buy; 🔵; ret5=-0.5; leftover $1415.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 1 | $16.47 | $0.19 | $-1.09 | $627.75 | ▲ $11,592.16 (+1,592.16) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 2 | $15.74 | $0.34 | $+8.65 | $658.89 | ▲ $11,591.82 (+1,591.82) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 9 | $2.32 | $0.26 | $-1.86 | $679.52 | ▲ $11,591.57 (+1,591.57) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 12 | $2.09 | $0.31 | $+1.35 | $704.29 | ▲ $11,591.26 (+1,591.26) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 18 | $1.75 | $0.39 | $+7.06 | $735.40 | ▲ $11,590.87 (+1,590.87) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 1 | $144.70 | $1.45 | — | $589.25 | ▲ $11,589.42 (+1,589.42) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $183.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 10 | $16.95 | $1.73 | — | $418.03 | ▲ $11,587.70 (+1,587.70) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $183.85 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 9 | $18.50 | $1.69 | — | $249.84 | ▲ $11,586.01 (+1,586.01) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $183.85 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 20 | $9.19 | $1.90 | — | $64.14 | ▲ $11,584.11 (+1,584.11) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $183.85 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $74.50 | ▲ $11,439.08 (+1,439.08) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.26 | $0.11 | $-0.21 | $82.65 | ▲ $11,438.97 (+1,438.97) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 2 | $4.69 | $0.12 | $-1.31 | $91.91 | ▲ $11,438.85 (+1,438.85) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 9 | $1.65 | $0.20 | $-0.10 | $106.57 | ▲ $11,438.66 (+1,438.66) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 7 | $1.78 | $0.17 | $-1.87 | $118.86 | ▲ $11,438.49 (+1,438.49) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 34 | $42.10 | $2.11 | $+42.71 | $1,548.15 | ▲ $11,543.54 (+1,543.54) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 57 | $24.70 | $2.18 | $-12.32 | $2,953.87 | ▲ $11,541.36 (+1,541.36) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 546 | $2.49 | $7.14 | $-68.79 | $4,306.26 | ▲ $11,534.21 (+1,534.21) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ACMR` | 17 | $70.52 | $2.06 | $-181.75 | $5,503.04 | ▲ $11,532.15 (+1,532.15) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GGB` | 320 | $4.81 | $4.19 | $+116.48 | $7,038.05 | ▲ $11,527.96 (+1,527.96) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MT` | 18 | $73.86 | $2.06 | $-26.79 | $8,365.46 | ▲ $11,525.89 (+1,525.89) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MU` | 1 | $930.83 | $2.01 | $+1.08 | $9,294.28 | ▲ $11,523.88 (+1,523.88) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $7,998.45 | ▲ $11,521.81 (+1,521.81) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $6,674.45 | ▲ $11,519.73 (+1,519.73) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 406 | $3.27 | $5.24 | — | $5,341.59 | ▲ $11,514.49 (+1,514.49) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 172 | $7.70 | $2.51 | — | $4,014.68 | ▲ $11,511.98 (+1,511.98) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $2,753.26 | ▲ $11,509.96 (+1,509.96) | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1327.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1088 | $1.22 | $14.04 | — | $1,411.87 | ▲ $11,495.93 (+1,495.93) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1327.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 72 | $18.40 | $2.21 | — | $84.86 | ▲ $11,493.72 (+1,493.72) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1327.75 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 100 | $15.45 | $2.32 | $+131.39 | $1,627.54 | ▲ $12,456.69 (+2,456.69) | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 1 | $137.70 | $1.40 | $-9.85 | $1,763.84 | ▲ $12,455.29 (+2,455.29) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 10 | $15.89 | $1.64 | $-13.96 | $1,921.10 | ▲ $12,453.65 (+2,453.65) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 9 | $17.31 | $1.60 | $-14.01 | $2,075.29 | ▲ $12,452.05 (+2,452.05) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 20 | $9.83 | $2.05 | $+8.86 | $2,269.84 | ▲ $12,450.00 (+2,450.00) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 1 | $266.94 | $1.99 | — | $2,000.91 | ▲ $12,448.01 (+2,448.01) | S≥+5: sizeup + more names; list flatten; ret5=+1.9; leftover $453.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 14 | $30.65 | $2.03 | — | $1,569.78 | ▲ $12,445.98 (+2,445.98) | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-2.2; leftover $453.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 43 | $10.41 | $2.12 | — | $1,120.03 | ▲ $12,443.86 (+2,443.86) | S≥+5: sizeup + more names; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $453.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 31 | $14.50 | $2.08 | — | $668.45 | ▲ $12,441.78 (+2,441.78) | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.8; leftover $453.97 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 232 | $1.95 | $2.99 | — | $213.05 | ▲ $12,438.78 (+2,438.78) | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $453.97 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TGTX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `SLS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `HIMS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `VOR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TLN` | cash | leftover split 10.32 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 10.32 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 10.32 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 10.32 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 10.32 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 10.32 < 1 share @ 14.80 |
| 2026-08-14 | `WWW` | cash | leftover split 10.32 < 1 share @ 20.60 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `VOR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 9.75 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 9.75 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 9.75 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.75 < 1 share @ 90.54 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `VOR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTSG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `IREN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TPG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TGTX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `SLS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `HIMS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `VOR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `MARA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `HNST` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `HNST` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 24.46 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 24.46 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 24.46 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BHP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 15.55 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 15.55 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 15.55 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `MOS` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OCUL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CRMD` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ACMR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ACMR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `GGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ACMR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `GGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `CAPR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 26 | 2026-09-03 @ $49.76 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1327.75 |
| `HRMY` | 32 | 2026-09-03 @ $41.31 | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1327.75 |
| `CABA` | 406 | 2026-09-03 @ $3.27 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1327.75 |
| `VSTM` | 172 | 2026-09-03 @ $7.70 | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1327.75 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | S≥+5: sizeup + more names; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1327.75 |
| `GPRO` | 1088 | 2026-09-03 @ $1.22 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1327.75 |
| `FRVO` | 72 | 2026-09-03 @ $18.40 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1327.75 |
| `ASND` | 1 | 2026-09-04 @ $266.94 | S≥+5: sizeup + more names; list flatten; ret5=+1.9; leftover $453.97 |
| `OSCR` | 14 | 2026-09-04 @ $30.65 | S≥+5: sizeup + more names; list flatten; 🔵; ret5=-2.2; leftover $453.97 |
| `NVAX` | 43 | 2026-09-04 @ $10.41 | S≥+5: sizeup + more names; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $453.97 |
| `BVS` | 31 | 2026-09-04 @ $14.50 | S≥+5: sizeup + more names; list flatten; 🔵; ⚪; ret5=+0.8; leftover $453.97 |
| `BAK` | 232 | 2026-09-04 @ $1.95 | S≥+5: sizeup + more names; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $453.97 |
