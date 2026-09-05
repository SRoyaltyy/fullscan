# Factor mine action — `union_h5_rankw`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `rank_w` · sell `list` · S-boost `none` · rank-weighted leftover

Cash book **+12.88%** ($11,288) · signal-only (no cash/fees) was +58.01%. Starts YES **16/17**. Fills 94 · skips 233 · realized $+916.73.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `rank_w` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $179.45.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $128.05 | $9,988.98 | $10,117.03 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | BUY BTSG x37 @ 59.80; BUY IREN x42 @ 45.98; BUY TPG x32 @ 50.62; BUY TGTX x27 @ 49.70; BUY SLS x94 @ 11.70; BUY HIMS x28 @ 29.74; BUY INO x685 @ 0.81; BUY TNDM x11 @ 23.33 |
| 2026-08-14 | +5.50 | $128.05 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11 | MARA, LDI, BTBT | — | $109.27 | $10,151.44 | $10,260.71 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2 | BUY MARA x1 @ 9.01; BUY LDI x7 @ 0.94; BUY BTBT x2 @ 1.50 |
| 2026-08-17 | +2.25 | $109.27 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2 | TMC, TGB, DNN | — | $85.16 | $10,205.01 | $10,290.17 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | BUY TMC x3 @ 4.05; BUY TGB x1 @ 8.46; BUY DNN x1 @ 3.24 |
| 2026-08-18 | -6.20 | $85.16 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | — | — | $85.16 | $10,109.65 | $10,194.81 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $85.16 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | — | — | $85.16 | $10,455.04 | $10,540.20 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $85.16 | BTSG×37, IREN×42, TPG×32, TGTX×27, SLS×94, HIMS×28, INO×685, TNDM×11, MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $45.26 | $10,621.52 | $10,666.78 | MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1, AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2 | SELL BTSG (dropped from list after 5 sess (min 5)); SELL IREN (dropped from list after 5 sess (min 5)); SELL TPG (dropped from list after 5 sess (min 5)); SELL TGTX (dropped from list after 5 sess (min 5)); SELL SLS (dropped from list after 5 sess (min 5)); SELL HIMS (dropped from list after 5 sess (min 5)); SELL INO (dropped from list after 5 sess (min 5)); SELL TNDM (dropped from list after 5 sess (min 5)); BUY AG x112 @ 20.55; BUY BHP x22 @ 91.01; BUY CDE x84 @ 20.65; BUY HDSN x250 @ 5.77; BUY IAG x58 @ 19.63; BUY KGC x29 @ 29.63; BUY NFGC x330 @ 1.75; BUY WPM x2 @ 144.54 |
| 2026-08-21 | +3.25 | $45.26 | MARA×1, LDI×7, BTBT×2, TMC×3, TGB×1, DNN×1, AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2 | AUTL, CRDL, CYPH | MARA, LDI, BTBT | $55.82 | $10,801.66 | $10,857.48 | TMC×3, TGB×1, DNN×1, AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1 | SELL MARA (dropped from list after 5 sess (min 5)); SELL LDI (dropped from list after 5 sess (min 5)); SELL BTBT (dropped from list after 5 sess (min 5)); BUY AUTL x2 @ 2.47; BUY CRDL x2 @ 1.93; BUY CYPH x1 @ 1.32 |
| 2026-08-24 | -5.17 | $55.82 | TMC×3, TGB×1, DNN×1, AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1 | — | TMC, TGB, DNN | $81.95 | $10,677.53 | $10,759.48 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1 | SELL TMC (dropped from list after 5 sess (min 5)); SELL TGB (dropped from list after 5 sess (min 5)); SELL DNN (dropped from list after 5 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $81.95 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1 | OCUL, CRMD, RZLT, BMEA, NPWR | — | $51.96 | $10,714.54 | $10,766.50 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1 | BUY OCUL x1 @ 10.92; BUY CRMD x1 @ 8.28; BUY RZLT x1 @ 5.23; BUY BMEA x2 @ 1.62; BUY NPWR x1 @ 2.00 |
| 2026-08-26 | +2.02 | $51.96 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1 | — | — | $51.96 | $10,761.25 | $10,813.21 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1 | hold AG,BHP,CDE,HDSN,IAG,KGC,NFGC,WPM,AUTL,CRDL,CYPH,OCUL,CRMD,RZLT,BMEA,NPWR |
| 2026-08-27 | — | $51.96 | AG×112, BHP×22, CDE×84, HDSN×250, IAG×58, KGC×29, NFGC×330, WPM×2, AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1 | RRC, CRK, MOS, SLI, ACMR, GGB, MT | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $481.39 | $10,395.74 | $10,877.13 | AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7 | SELL AG (dropped from list after 5 sess (min 5)); SELL BHP (dropped from list after 5 sess (min 5)); SELL CDE (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL WPM (dropped from list after 5 sess (min 5)); BUY RRC x58 @ 40.72; BUY CRK x149 @ 14.09; BUY MOS x72 @ 24.84; BUY SLI x579 @ 2.59; BUY ACMR x14 @ 80.97; BUY GGB x203 @ 4.42; BUY MT x7 @ 75.12 |
| 2026-08-28 | +0.75 | $481.39 | AUTL×2, CRDL×2, CYPH×1, OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7 | ANF, BHVN, BZ, CAPR | AUTL, CRDL, CYPH | $68.79 | $10,880.52 | $10,949.31 | OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | SELL AUTL (dropped from list after 5 sess (min 5)); SELL CRDL (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)); BUY ANF x1 @ 144.70; BUY BHVN x8 @ 16.95; BUY BZ x5 @ 18.50; BUY CAPR x5 @ 9.19 |
| 2026-08-31 | -5.85 | $68.79 | OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | — | — | $68.79 | $10,687.33 | $10,756.12 | OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $68.79 | OCUL×1, CRMD×1, RZLT×1, BMEA×2, NPWR×1, RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | — | OCUL, CRMD, RZLT, BMEA, NPWR | $96.90 | $10,784.26 | $10,881.16 | RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | SELL OCUL (dropped from list after 5 sess (min 5)); SELL CRMD (dropped from list after 5 sess (min 5)); SELL RZLT (dropped from list after 5 sess (min 5)); SELL BMEA (dropped from list after 5 sess (min 5)); SELL NPWR (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $96.90 | RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | — | — | $96.90 | $10,922.25 | $11,019.15 | RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $96.90 | RRC×58, CRK×149, MOS×72, SLI×579, ACMR×14, GGB×203, MT×7, ANF×1, BHVN×8, BZ×5, CAPR×5 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO | RRC, MOS, SLI, ACMR, GGB, MT | $169.09 | $11,356.47 | $11,525.56 | CRK×149, ANF×1, BHVN×8, BZ×5, CAPR×5, ATRC×41, HRMY×42, CABA×448, VSTM×152, RVTY×6, GPRO×481, FRVO×15 | SELL RRC (dropped from list after 5 sess (min 5)); SELL MOS (dropped from list after 5 sess (min 5)); SELL SLI (dropped from list after 5 sess (min 5)); SELL ACMR (dropped from list after 5 sess (min 5)); SELL GGB (dropped from list after 5 sess (min 5)); SELL MT (dropped from list after 5 sess (min 5)); BUY ATRC x41 @ 49.76; BUY HRMY x42 @ 41.31; BUY CABA x448 @ 3.27; BUY VSTM x152 @ 7.70; BUY RVTY x6 @ 125.94; BUY GPRO x481 @ 1.22; BUY FRVO x15 @ 18.40 |
| 2026-09-04 | — | $169.09 | CRK×149, ANF×1, BHVN×8, BZ×5, CAPR×5, ATRC×41, HRMY×42, CABA×448, VSTM×152, RVTY×6, GPRO×481, FRVO×15 | ASND, OSCR, NVAX, BVS, BAK | CRK, ANF, BHVN, BZ, CAPR | $179.45 | $11,108.11 | $11,287.56 | ATRC×41, HRMY×42, CABA×448, VSTM×152, RVTY×6, GPRO×481, FRVO×15, ASND×3, OSCR×24, NVAX×55, BVS×26, BAK×97 | SELL CRK (dropped from list after 6 sess (min 5)); SELL ANF (dropped from list after 5 sess (min 5)); SELL BHVN (dropped from list after 5 sess (min 5)); SELL BZ (dropped from list after 5 sess (min 5)); SELL CAPR (dropped from list after 5 sess (min 5)); BUY ASND x3 @ 266.94; BUY OSCR x24 @ 30.65; BUY NVAX x55 @ 10.41; BUY BVS x26 @ 14.50; BUY BAK x97 @ 1.95 |

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
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $118.95 | ▲ $10,103.33 (+103.33) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-13.5; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $112.30 | ▲ $10,103.24 (+103.24) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.11 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 2 | $1.50 | $0.04 | — | $109.27 | ▲ $10,103.21 (+103.21) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+9.2; leftover $3.56 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 3 | $4.05 | $0.13 | — | $96.99 | ▲ $10,281.04 (+281.04) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=-12.3; leftover $15.18 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $88.44 | ▲ $10,280.96 (+280.96) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.4; leftover $12.14 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 1 | $3.24 | $0.04 | — | $85.16 | ▲ $10,280.92 (+280.92) | rank-weighted leftover; list flatten; ⚪; ret5=+0.3; leftover $6.07 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 37 | $58.64 | $2.13 | $-47.15 | $2,252.71 | ▲ $10,475.18 (+475.18) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 42 | $42.46 | $2.14 | $-152.10 | $4,033.89 | ▲ $10,473.04 (+473.04) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 32 | $53.06 | $2.11 | $+73.78 | $5,729.70 | ▲ $10,470.93 (+470.93) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGTX` | 27 | $51.65 | $2.09 | $+48.49 | $7,122.16 | ▲ $10,468.84 (+468.84) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `SLS` | 94 | $13.84 | $2.30 | $+196.59 | $8,420.82 | ▲ $10,466.54 (+466.54) | dropped from list after 5 sess (min 5) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `HIMS` | 28 | $30.66 | $2.09 | $+21.59 | $9,277.21 | ▲ $10,464.45 (+464.45) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 685 | $1.30 | $8.96 | $+319.09 | $10,158.75 | ▲ $10,455.49 (+455.49) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 11 | $23.11 | $2.04 | $-6.49 | $10,410.92 | ▲ $10,453.44 (+453.44) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 112 | $20.55 | $2.33 | — | $8,106.99 | ▲ $10,451.12 (+451.12) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $2313.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 22 | $91.01 | $2.06 | — | $6,102.72 | ▲ $10,449.06 (+449.06) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $2024.35 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 84 | $20.65 | $2.24 | — | $4,365.87 | ▲ $10,446.82 (+446.82) | rank-weighted leftover; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1735.15 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 250 | $5.77 | $3.23 | — | $2,920.15 | ▲ $10,443.60 (+443.60) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1445.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 58 | $19.63 | $2.16 | — | $1,779.44 | ▲ $10,441.43 (+441.43) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1156.77 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $918.10 | ▲ $10,439.35 (+439.35) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $867.58 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 330 | $1.75 | $4.26 | — | $336.34 | ▲ $10,435.10 (+435.10) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $578.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 2 | $144.54 | $2.00 | — | $45.26 | ▲ $10,433.10 (+433.10) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $289.19 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `MARA` | 1 | $11.70 | $0.14 | $+2.46 | $56.82 | ▲ $10,954.77 (+954.77) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 7 | $0.87 | $0.10 | $-0.68 | $62.79 | ▲ $10,954.67 (+954.67) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 2 | $1.66 | $0.06 | $+0.22 | $66.05 | ▲ $10,954.61 (+954.61) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 2 | $2.47 | $0.06 | — | $61.06 | ▲ $10,954.56 (+954.56) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $7.34 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 2 | $1.93 | $0.04 | — | $57.15 | ▲ $10,954.51 (+954.51) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $5.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1 | $1.32 | $0.02 | — | $55.82 | ▲ $10,954.50 (+954.50) | rank-weighted leftover; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1.83 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TMC` | 3 | $4.57 | $0.17 | $+1.26 | $69.36 | ▲ $10,985.20 (+985.20) | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `TGB` | 1 | $9.26 | $0.12 | $+0.60 | $78.50 | ▲ $10,985.08 (+985.08) | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `DNN` | 1 | $3.50 | $0.06 | $+0.17 | $81.95 | ▲ $10,985.03 (+985.03) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🔴 news🟡 digest🟢 judge🟡 ab🟡 peer🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 1 | $10.92 | $0.11 | — | $70.91 | ▲ $10,813.41 (+813.41) | rank-weighted leftover; list flatten; 🔵; ret5=+10.4; leftover $15.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 1 | $8.28 | $0.09 | — | $62.55 | ▲ $10,813.33 (+813.33) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+8.8; leftover $11.38 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 1 | $5.23 | $0.06 | — | $57.26 | ▲ $10,813.27 (+813.27) | rank-weighted leftover; list flatten; ret5=+10.7; leftover $9.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 2 | $1.62 | $0.04 | — | $53.98 | ▲ $10,813.23 (+813.23) | rank-weighted leftover; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $4.55 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 1 | $2.00 | $0.02 | — | $51.96 | ▲ $10,813.21 (+813.21) | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $2.28 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 112 | $20.63 | $2.36 | $+4.27 | $2,360.16 | ▲ $10,866.83 (+866.83) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `BHP` | 22 | $96.99 | $2.08 | $+127.42 | $4,491.86 | ▲ $10,864.75 (+864.75) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 84 | $21.00 | $2.27 | $+24.89 | $6,253.59 | ▲ $10,862.48 (+862.48) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 250 | $5.51 | $3.28 | $-71.50 | $7,627.81 | ▲ $10,859.20 (+859.20) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 58 | $21.64 | $2.18 | $+112.23 | $8,880.74 | ▲ $10,857.01 (+857.01) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 29 | $32.90 | $2.10 | $+90.66 | $9,832.75 | ▲ $10,854.92 (+854.92) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 330 | $2.00 | $4.32 | $+73.92 | $10,488.43 | ▲ $10,850.60 (+850.60) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 2 | $160.93 | $2.02 | $+28.77 | $10,808.27 | ▲ $10,848.58 (+848.58) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 58 | $40.72 | $2.16 | — | $8,444.35 | ▲ $10,846.42 (+846.42) | rank-weighted leftover; list flatten; ret5=+1.8; leftover $2401.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 149 | $14.09 | $2.44 | — | $6,342.50 | ▲ $10,843.98 (+843.98) | rank-weighted leftover; list flatten; ret5=+1.1; leftover $2101.61 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 72 | $24.84 | $2.21 | — | $4,551.81 | ▲ $10,841.77 (+841.77) | rank-weighted leftover; list flatten; ret5=+13.0; leftover $1801.38 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 579 | $2.59 | $7.47 | — | $3,044.73 | ▲ $10,834.30 (+834.30) | rank-weighted leftover; list flatten; ret5=+4.2; leftover $1501.15 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 14 | $80.97 | $2.03 | — | $1,909.12 | ▲ $10,832.27 (+832.27) | rank-weighted leftover; list mover_buy; 🔵; ret5=-1.3; leftover $1200.92 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 203 | $4.42 | $2.62 | — | $1,009.24 | ▲ $10,829.65 (+829.65) | rank-weighted leftover; list mover_buy; 🔵; ret5=-8.6; leftover $900.69 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 7 | $75.12 | $2.01 | — | $481.39 | ▲ $10,827.64 (+827.64) | rank-weighted leftover; list mover_buy; 🔵; ret5=-2.2; leftover $600.46 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 2 | $2.32 | $0.07 | $-0.43 | $485.96 | ▲ $10,899.48 (+899.48) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 2 | $2.09 | $0.07 | $+0.21 | $490.07 | ▲ $10,899.41 (+899.41) | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 1 | $1.75 | $0.04 | $+0.37 | $491.78 | ▲ $10,899.37 (+899.37) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 1 | $144.70 | $1.45 | — | $345.63 | ▲ $10,897.92 (+897.92) | rank-weighted leftover; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $196.71 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 8 | $16.95 | $1.38 | — | $208.65 | ▲ $10,896.54 (+896.54) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $147.53 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 5 | $18.50 | $0.94 | — | $115.21 | ▲ $10,895.60 (+895.60) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $98.36 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 5 | $9.19 | $0.47 | — | $68.79 | ▲ $10,895.13 (+895.13) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $49.18 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `OCUL` | 1 | $10.49 | $0.13 | $-0.67 | $79.15 | ▲ $10,775.66 (+775.66) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRMD` | 1 | $8.26 | $0.11 | $-0.21 | $87.30 | ▲ $10,775.55 (+775.55) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 1 | $4.69 | $0.07 | $-0.67 | $91.92 | ▲ $10,775.48 (+775.48) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 2 | $1.65 | $0.06 | $-0.04 | $95.16 | ▲ $10,775.42 (+775.42) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 1 | $1.78 | $0.04 | $-0.28 | $96.90 | ▲ $10,775.38 (+775.38) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 58 | $42.10 | $2.19 | $+75.68 | $2,536.51 | ▲ $10,982.66 (+982.66) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 72 | $24.70 | $2.23 | $-14.52 | $4,312.68 | ▲ $10,980.43 (+980.43) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 579 | $2.49 | $7.58 | $-72.95 | $5,746.81 | ▲ $10,972.85 (+972.85) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ACMR` | 14 | $70.52 | $2.05 | $-150.38 | $6,732.04 | ▲ $10,970.80 (+970.80) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GGB` | 203 | $4.81 | $2.66 | $+73.89 | $7,705.81 | ▲ $10,968.14 (+968.14) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MT` | 7 | $73.86 | $2.03 | $-12.86 | $8,220.80 | ▲ $10,966.11 (+966.11) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 41 | $49.76 | $2.11 | — | $6,178.52 | ▲ $10,963.99 (+963.99) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2055.20 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 42 | $41.31 | $2.12 | — | $4,441.39 | ▲ $10,961.88 (+961.88) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1761.60 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 448 | $3.27 | $5.78 | — | $2,970.65 | ▲ $10,956.10 (+956.10) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1468.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 152 | $7.70 | $2.45 | — | $1,797.80 | ▲ $10,953.65 (+953.65) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1174.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 6 | $125.94 | $2.01 | — | $1,040.15 | ▲ $10,951.64 (+951.64) | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $880.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 481 | $1.22 | $6.20 | — | $447.13 | ▲ $10,945.44 (+945.44) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $587.20 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 15 | $18.40 | $2.04 | — | $169.09 | ▲ $10,943.40 (+943.40) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $293.60 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 149 | $15.45 | $2.48 | $+197.72 | $2,468.66 | ▲ $11,612.05 (+1,612.05) | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 1 | $137.70 | $1.40 | $-9.85 | $2,604.96 | ▲ $11,610.65 (+1,610.65) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 8 | $15.89 | $1.32 | $-11.18 | $2,730.77 | ▲ $11,609.34 (+1,609.34) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 5 | $17.31 | $0.90 | $-7.79 | $2,816.42 | ▲ $11,608.44 (+1,608.44) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `CAPR` | 5 | $9.83 | $0.53 | $+2.20 | $2,865.04 | ▲ $11,607.91 (+1,607.91) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `ASND` | 3 | $266.94 | $2.00 | — | $2,062.22 | ▲ $11,605.91 (+1,605.91) | rank-weighted leftover; list flatten; ret5=+1.9; leftover $955.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 24 | $30.65 | $2.06 | — | $1,324.56 | ▲ $11,603.85 (+1,603.85) | rank-weighted leftover; list flatten; 🔵; ret5=-2.2; leftover $764.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 55 | $10.41 | $2.15 | — | $749.85 | ▲ $11,601.69 (+1,601.69) | rank-weighted leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $573.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 26 | $14.50 | $2.07 | — | $370.79 | ▲ $11,599.63 (+1,599.63) | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $382.01 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 97 | $1.95 | $2.18 | — | $179.45 | ▲ $11,597.44 (+1,597.44) | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $191.00 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 28.46 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 24.90 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 21.34 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 17.78 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 14.23 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 24.28 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 21.25 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 18.21 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 9.11 < 1 share @ 90.54 |
| 2026-08-17 | `HNST` | cash | leftover split 3.04 < 1 share @ 4.81 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TGTX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `SLS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `HIMS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| 2026-08-19 | `MARA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-20 | `TMC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `TGB` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `DNN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-21 | `TMC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `TGB` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 14.68 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 12.84 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 11.01 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.17 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 3.67 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 18.21 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 13.66 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 6.83 < 1 share @ 429.24 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BHP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `MU` | cash | leftover split 300.23 < 1 share @ 925.74 |
| 2026-08-28 | `OCUL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `CRMD` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ACMR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `MT` | min_hold | dropped but min-hold 1/5 sess — no sell |
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
| `ATRC` | 41 | 2026-09-03 @ $49.76 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+10.6; leftover $2055.20 |
| `HRMY` | 42 | 2026-09-03 @ $41.31 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1761.60 |
| `CABA` | 448 | 2026-09-03 @ $3.27 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1468.00 |
| `VSTM` | 152 | 2026-09-03 @ $7.70 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1174.40 |
| `RVTY` | 6 | 2026-09-03 @ $125.94 | rank-weighted leftover; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $880.80 |
| `GPRO` | 481 | 2026-09-03 @ $1.22 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $587.20 |
| `FRVO` | 15 | 2026-09-03 @ $18.40 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $293.60 |
| `ASND` | 3 | 2026-09-04 @ $266.94 | rank-weighted leftover; list flatten; ret5=+1.9; leftover $955.01 |
| `OSCR` | 24 | 2026-09-04 @ $30.65 | rank-weighted leftover; list flatten; 🔵; ret5=-2.2; leftover $764.01 |
| `NVAX` | 55 | 2026-09-04 @ $10.41 | rank-weighted leftover; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $573.01 |
| `BVS` | 26 | 2026-09-04 @ $14.50 | rank-weighted leftover; list flatten; 🔵; ⚪; ret5=+0.8; leftover $382.01 |
| `BAK` | 97 | 2026-09-04 @ $1.95 | rank-weighted leftover; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $191.00 |
