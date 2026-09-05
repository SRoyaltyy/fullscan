# Factor mine action — `union_h3_topheavy`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `topheavy` · sell `list` · S-boost `none` · 40% to #1, rest split

Cash book **+10.51%** ($11,051) · signal-only (no cash/fees) was +34.19%. Starts YES **15/17**. Fills 96 · skips 155 · realized $+688.25.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `topheavy` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $68.26.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | — | $160.57 | $9,962.48 | $10,123.05 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | BUY BTSG x66 @ 59.80; BUY IREN x18 @ 45.98; BUY TPG x16 @ 50.62; BUY TGTX x17 @ 49.70; BUY SLS x73 @ 11.70; BUY HIMS x28 @ 29.74; BUY INO x1058 @ 0.81; BUY TNDM x36 @ 23.33 |
| 2026-08-14 | +5.50 | $160.57 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36 | MARA, LDI, BTBT | — | $124.52 | $10,271.17 | $10,395.68 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9 | BUY MARA x1 @ 9.01; BUY LDI x14 @ 0.94; BUY BTBT x9 @ 1.50 |
| 2026-08-17 | +2.25 | $124.52 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9 | DVN, TMC, TGB, DNN, HNST | — | $41.59 | $10,346.54 | $10,388.13 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | BUY DVN x1 @ 46.18; BUY TMC x2 @ 4.05; BUY TGB x1 @ 8.46; BUY DNN x3 @ 3.24; BUY HNST x2 @ 4.81 |
| 2026-08-18 | -6.20 | $41.59 | BTSG×66, IREN×18, TPG×16, TGTX×17, SLS×73, HIMS×28, INO×1058, TNDM×36, MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | — | BTSG, IREN, TPG, TGTX, SLS, HIMS, INO, TNDM | $10,130.92 | $117.00 | $10,247.92 | MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | SELL BTSG (dropped from list after 3 sess (min 3)); SELL IREN (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL TGTX (dropped from list after 3 sess (min 3)); SELL SLS (dropped from list after 3 sess (min 3)); SELL HIMS (dropped from list after 3 sess (min 3)); SELL INO (dropped from list after 3 sess (min 3)); SELL TNDM (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,130.92 | MARA×1, LDI×14, BTBT×9, DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | — | MARA, LDI, BTBT | $10,164.46 | $84.30 | $10,248.76 | DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | SELL MARA (dropped from list after 3 sess (min 3)); SELL LDI (dropped from list after 3 sess (min 3)); SELL BTBT (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,164.46 | DVN×1, TMC×2, TGB×1, DNN×3, HNST×2 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, TMC, TGB, DNN, HNST | $106.56 | $10,384.46 | $10,491.02 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6 | SELL DVN (dropped from list after 3 sess (min 3)); SELL TMC (dropped from list after 3 sess (min 3)); SELL TGB (dropped from list after 3 sess (min 3)); SELL DNN (dropped from list after 3 sess (min 3)); SELL HNST (dropped from list after 3 sess (min 3)); BUY AG x199 @ 20.55; BUY BHP x9 @ 91.01; BUY CDE x42 @ 20.65; BUY HDSN x152 @ 5.77; BUY IAG x44 @ 19.63; BUY KGC x29 @ 29.63; BUY NFGC x501 @ 1.75; BUY WPM x6 @ 144.54 |
| 2026-08-21 | +3.25 | $106.56 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6 | AUTL, CRDL, CYPH | — | $83.24 | $10,578.59 | $10,661.83 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6, AUTL×3, CRDL×4, CYPH×6 | BUY AUTL x3 @ 2.47; BUY CRDL x4 @ 1.93; BUY CYPH x6 @ 1.32 |
| 2026-08-24 | -5.17 | $83.24 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6, AUTL×3, CRDL×4, CYPH×6 | — | — | $83.24 | $10,476.14 | $10,559.38 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6, AUTL×3, CRDL×4, CYPH×6 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $83.24 | AG×199, BHP×9, CDE×42, HDSN×152, IAG×44, KGC×29, NFGC×501, WPM×6, AUTL×3, CRDL×4, CYPH×6 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $88.25 | $10,462.75 | $10,551.00 | AUTL×3, CRDL×4, CYPH×6, MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453 | SELL AG (dropped from list after 3 sess (min 3)); SELL BHP (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL HDSN (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); BUY MOS x176 @ 24.00; BUY OCUL x83 @ 10.92; BUY INSP x14 @ 61.47; BUY CRMD x109 @ 8.28; BUY RZLT x173 @ 5.23; BUY HCA x2 @ 429.24; BUY BMEA x560 @ 1.62; BUY NPWR x453 @ 2.00 |
| 2026-08-26 | +2.02 | $88.25 | AUTL×3, CRDL×4, CYPH×6, MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453 | — | — | $88.25 | $10,494.69 | $10,582.94 | AUTL×3, CRDL×4, CYPH×6, MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453 | hold AUTL,CRDL,CYPH,MOS,OCUL,INSP,CRMD,RZLT,HCA,BMEA,NPWR |
| 2026-08-27 | — | $88.25 | AUTL×3, CRDL×4, CYPH×6, MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453 | RRC, SLI, GGB | AUTL, CRDL, CYPH | $52.31 | $10,490.46 | $10,542.77 | MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453, RRC×1, SLI×4, GGB×2 | SELL AUTL (dropped from list after 4 sess (min 3)); SELL CRDL (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); BUY RRC x1 @ 40.72; BUY SLI x4 @ 2.59; BUY GGB x2 @ 4.42 |
| 2026-08-28 | +0.75 | $52.31 | MOS×176, OCUL×83, INSP×14, CRMD×109, RZLT×173, HCA×2, BMEA×560, NPWR×453, RRC×1, SLI×4, GGB×2 | CRK, ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $89.67 | $10,435.16 | $10,524.83 | MOS×176, RRC×1, SLI×4, GGB×2, CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | SELL OCUL (dropped from list after 3 sess (min 3)); SELL INSP (dropped from list after 3 sess (min 3)); SELL CRMD (dropped from list after 3 sess (min 3)); SELL RZLT (dropped from list after 3 sess (min 3)); SELL HCA (dropped from list after 3 sess (min 3)); SELL BMEA (dropped from list after 3 sess (min 3)); SELL NPWR (dropped from list after 3 sess (min 3)); BUY CRK x172 @ 14.42; BUY ANF x6 @ 144.70; BUY BHVN x55 @ 16.95; BUY BZ x50 @ 18.50; BUY CAPR x101 @ 9.19 |
| 2026-08-31 | -5.85 | $89.67 | MOS×176, RRC×1, SLI×4, GGB×2, CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | — | MOS | $4,267.09 | $6,139.68 | $10,406.77 | RRC×1, SLI×4, GGB×2, CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | SELL MOS (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $4,267.09 | RRC×1, SLI×4, GGB×2, CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | — | RRC, SLI, GGB | $4,327.73 | $6,158.24 | $10,485.97 | CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | SELL RRC (dropped from list after 3 sess (min 3)); SELL SLI (dropped from list after 3 sess (min 3)); SELL GGB (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $4,327.73 | CRK×172, ANF×6, BHVN×55, BZ×50, CAPR×101 | — | CRK, ANF, BHVN, BZ, CAPR | $10,688.25 | $0.00 | $10,688.25 | — | SELL CRK (dropped from list after 3 sess (min 3)); SELL ANF (dropped from list after 3 sess (min 3)); SELL BHVN (dropped from list after 3 sess (min 3)); SELL BZ (dropped from list after 3 sess (min 3)); SELL CAPR (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,688.25 | — | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $90.61 | $11,325.45 | $11,416.06 | ATRC×85, HRMY×22, CABA×280, VSTM×118, RVTY×7, GPRO×750, FRVO×49, CRK×58 | BUY ATRC x85 @ 49.76; BUY HRMY x22 @ 41.31; BUY CABA x280 @ 3.27; BUY VSTM x118 @ 7.70; BUY RVTY x7 @ 125.94; BUY GPRO x750 @ 1.22; BUY FRVO x49 @ 18.40; BUY CRK x58 @ 15.70 |
| 2026-09-04 | — | $90.61 | ATRC×85, HRMY×22, CABA×280, VSTM×118, RVTY×7, GPRO×750, FRVO×49, CRK×58 | NVAX, BAK | — | $68.26 | $10,982.89 | $11,051.15 | ATRC×85, HRMY×22, CABA×280, VSTM×118, RVTY×7, GPRO×750, FRVO×49, CRK×58, NVAX×1, BAK×6 | BUY NVAX x1 @ 10.41; BUY BAK x6 @ 1.95 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 66 | $59.80 | $2.19 | — | $6,051.01 | ▼ $9,997.81 (-2.19) | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $4000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 18 | $45.98 | $2.04 | — | $5,221.33 | ▼ $9,995.77 (-4.23) | 40% to #1, rest split; list flatten; ⚪; ret5=+12.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 16 | $50.62 | $2.04 | — | $4,409.32 | ▼ $9,993.73 (-6.27) | 40% to #1, rest split; list flatten; ⚪; ret5=+6.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TGTX` | 17 | $49.70 | $2.04 | — | $3,562.38 | ▼ $9,991.69 (-8.31) | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `SLS` | 73 | $11.70 | $2.21 | — | $2,706.07 | ▼ $9,989.48 (-10.52) | 40% to #1, rest split; list flatten; ⚪; ret5=-0.8; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `HIMS` | 28 | $29.74 | $2.07 | — | $1,871.27 | ▼ $9,987.41 (-12.59) | 40% to #1, rest split; list flatten; ⚪; ret5=-5.3; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 1058 | $0.81 | $11.74 | — | $1,002.55 | ▼ $9,975.66 (-24.34) | 40% to #1, rest split; list flatten; ⚪; ret5=+13.2; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 36 | $23.33 | $2.10 | — | $160.57 | ▼ $9,973.56 (-26.44) | 40% to #1, rest split; list flatten; ⚪; ret5=+19.7; leftover $857.14 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 1 | $9.01 | $0.09 | — | $151.47 | ▲ $10,109.69 (+109.69) | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-13.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 14 | $0.94 | $0.17 | — | $138.18 | ▲ $10,109.52 (+109.52) | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.5; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 9 | $1.50 | $0.16 | — | $124.52 | ▲ $10,109.35 (+109.35) | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+9.2; leftover $13.76 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $77.87 | ▲ $10,379.55 (+379.55) | 40% to #1, rest split; list flatten; 🔵; ret5=+6.7; leftover $49.81 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 2 | $4.05 | $0.09 | — | $69.68 | ▲ $10,379.46 (+379.46) | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=-12.3; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 1 | $8.46 | $0.09 | — | $61.14 | ▲ $10,379.38 (+379.38) | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+0.4; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 3 | $3.24 | $0.11 | — | $51.31 | ▲ $10,379.27 (+379.27) | 40% to #1, rest split; list flatten; ⚪; ret5=+0.3; leftover $10.67 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HNST` | 2 | $4.81 | $0.10 | — | $41.59 | ▲ $10,379.17 (+379.17) | 40% to #1, rest split; list flatten; ⚪; ret5=-11.4; leftover $10.67 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 66 | $60.00 | $2.23 | $+8.78 | $3,999.36 | ▲ $10,275.44 (+275.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 18 | $43.56 | $2.06 | $-47.67 | $4,781.37 | ▲ $10,273.37 (+273.37) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 16 | $51.77 | $2.06 | $+14.25 | $5,607.64 | ▲ $10,271.32 (+271.32) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGTX` | 17 | $49.28 | $2.06 | $-11.24 | $6,443.34 | ▲ $10,269.26 (+269.26) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `SLS` | 73 | $12.66 | $2.23 | $+65.64 | $7,365.28 | ▲ $10,267.02 (+267.02) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `HIMS` | 28 | $27.85 | $2.09 | $-57.09 | $8,142.99 | ▲ $10,264.93 (+264.93) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 1058 | $1.14 | $13.83 | $+323.56 | $9,335.28 | ▲ $10,251.10 (+251.10) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 36 | $22.16 | $2.12 | $-46.34 | $10,130.92 | ▲ $10,248.98 (+248.98) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 1 | $8.91 | $0.11 | $-0.31 | $10,139.72 | ▲ $10,248.77 (+248.77) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 14 | $0.88 | $0.19 | $-1.16 | $10,151.85 | ▲ $10,248.58 (+248.58) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 9 | $1.42 | $0.17 | $-1.06 | $10,164.46 | ▲ $10,248.41 (+248.41) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $10,212.96 | ▲ $10,248.71 (+248.71) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 2 | $3.92 | $0.10 | $-0.45 | $10,220.70 | ▲ $10,248.61 (+248.61) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 1 | $8.35 | $0.11 | $-0.30 | $10,228.94 | ▲ $10,248.50 (+248.50) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 3 | $3.20 | $0.12 | $-0.35 | $10,238.42 | ▲ $10,248.38 (+248.38) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `HNST` | 2 | $4.98 | $0.13 | $+0.11 | $10,248.25 | ▲ $10,248.25 (+248.25) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 199 | $20.55 | $2.59 | — | $6,156.21 | ▲ $10,245.66 (+245.66) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $4099.30 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 9 | $91.01 | $2.02 | — | $5,335.11 | ▲ $10,243.65 (+243.65) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 42 | $20.65 | $2.12 | — | $4,465.69 | ▲ $10,241.53 (+241.53) | 40% to #1, rest split; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 152 | $5.77 | $2.45 | — | $3,586.21 | ▲ $10,239.09 (+239.09) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 44 | $19.63 | $2.12 | — | $2,720.36 | ▲ $10,236.96 (+236.96) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 29 | $29.63 | $2.08 | — | $1,859.02 | ▲ $10,234.89 (+234.89) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 501 | $1.75 | $6.46 | — | $975.80 | ▲ $10,228.42 (+228.42) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 6 | $144.54 | $2.01 | — | $106.56 | ▲ $10,226.42 (+226.42) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $878.42 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 3 | $2.47 | $0.08 | — | $99.06 | ▲ $10,790.79 (+790.79) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $9.13 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 4 | $1.93 | $0.09 | — | $91.25 | ▲ $10,790.70 (+790.70) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $9.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 6 | $1.32 | $0.10 | — | $83.24 | ▲ $10,790.61 (+790.61) | 40% to #1, rest split; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $9.13 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 199 | $20.73 | $2.65 | $+30.58 | $4,205.85 | ▲ $10,629.09 (+629.09) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 9 | $95.95 | $2.04 | $+40.41 | $5,067.37 | ▲ $10,627.06 (+627.06) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 42 | $20.85 | $2.14 | $+4.15 | $5,940.93 | ▲ $10,624.92 (+624.92) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 152 | $5.53 | $2.48 | $-41.41 | $6,779.01 | ▲ $10,622.44 (+622.44) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 44 | $21.63 | $2.14 | $+83.74 | $7,728.59 | ▲ $10,620.30 (+620.30) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 29 | $32.76 | $2.10 | $+86.60 | $8,676.53 | ▲ $10,618.20 (+618.20) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 501 | $1.91 | $6.56 | $+67.14 | $9,626.88 | ▲ $10,611.64 (+611.64) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 6 | $160.00 | $2.03 | $+88.72 | $10,584.86 | ▲ $10,609.62 (+609.62) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 176 | $24.00 | $2.52 | — | $6,358.34 | ▲ $10,607.10 (+607.10) | 40% to #1, rest split; list flatten; ⚪; ret5=+13.0; leftover $4233.94 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 83 | $10.92 | $2.24 | — | $5,449.74 | ▲ $10,604.86 (+604.86) | 40% to #1, rest split; list flatten; 🔵; ret5=+10.4; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 14 | $61.47 | $2.03 | — | $4,587.13 | ▲ $10,602.83 (+602.83) | 40% to #1, rest split; list flatten; 🔵; ret5=+9.2; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 109 | $8.28 | $2.32 | — | $3,682.29 | ▲ $10,600.51 (+600.51) | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+8.8; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 173 | $5.23 | $2.51 | — | $2,774.99 | ▲ $10,598.00 (+598.00) | 40% to #1, rest split; list flatten; ret5=+10.7; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $1,914.51 | ▲ $10,596.00 (+596.00) | 40% to #1, rest split; list flatten; ret5=+6.1; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 560 | $1.62 | $7.22 | — | $1,000.09 | ▲ $10,588.78 (+588.78) | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $907.27 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 453 | $2.00 | $5.84 | — | $88.25 | ▲ $10,582.94 (+582.94) | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $907.27 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 3 | $2.41 | $0.10 | $-0.36 | $95.38 | ▲ $10,734.91 (+734.91) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 4 | $2.03 | $0.11 | $+0.20 | $103.38 | ▲ $10,734.79 (+734.79) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 6 | $1.60 | $0.13 | $+1.45 | $112.85 | ▲ $10,734.66 (+734.66) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $71.72 | ▲ $10,734.25 (+734.25) | 40% to #1, rest split; list flatten; ret5=+1.8; leftover $45.14 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 4 | $2.59 | $0.12 | — | $61.24 | ▲ $10,734.13 (+734.13) | 40% to #1, rest split; list flatten; ret5=+4.2; leftover $11.28 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 2 | $4.42 | $0.09 | — | $52.31 | ▲ $10,734.04 (+734.04) | 40% to #1, rest split; list mover_buy; 🔵; ret5=-8.6; leftover $11.28 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 83 | $10.63 | $2.26 | $-28.57 | $932.34 | ▲ $10,541.85 (+541.85) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 14 | $62.10 | $2.05 | $+4.74 | $1,799.68 | ▲ $10,539.79 (+539.79) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 109 | $8.49 | $2.35 | $+18.23 | $2,722.75 | ▲ $10,537.45 (+537.45) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 173 | $5.07 | $2.55 | $-32.74 | $3,597.31 | ▲ $10,534.90 (+534.90) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-13.27 | $4,444.51 | ▲ $10,532.88 (+532.88) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 560 | $1.74 | $7.33 | $+52.65 | $5,411.59 | ▲ $10,525.56 (+525.56) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 453 | $1.83 | $5.93 | $-88.78 | $6,234.65 | ▲ $10,519.63 (+519.63) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 172 | $14.42 | $2.51 | — | $3,751.90 | ▲ $10,517.12 (+517.12) | 40% to #1, rest split; list flatten; ret5=+1.1; leftover $2493.86 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 6 | $144.70 | $2.01 | — | $2,881.69 | ▲ $10,515.11 (+515.11) | 40% to #1, rest split; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $935.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 55 | $16.95 | $2.15 | — | $1,947.29 | ▲ $10,512.96 (+512.96) | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $935.20 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 50 | $18.50 | $2.14 | — | $1,020.15 | ▲ $10,510.82 (+510.82) | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $935.20 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 101 | $9.19 | $2.29 | — | $89.67 | ▲ $10,508.53 (+508.53) | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $935.20 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 176 | $23.75 | $2.58 | $-49.10 | $4,267.09 | ▲ $10,420.82 (+420.82) | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $4,307.97 | ▲ $10,415.81 (+415.81) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 4 | $2.70 | $0.14 | $+0.18 | $4,318.63 | ▲ $10,415.67 (+415.67) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GGB` | 2 | $4.61 | $0.12 | $+0.17 | $4,327.73 | ▲ $10,415.55 (+415.55) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 172 | $15.82 | $2.56 | $+235.74 | $7,046.22 | ▲ $10,696.94 (+696.94) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 6 | $142.00 | $2.03 | $-20.24 | $7,896.19 | ▲ $10,694.91 (+694.91) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 55 | $15.39 | $2.17 | $-90.13 | $8,740.46 | ▲ $10,692.73 (+692.73) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 50 | $17.29 | $2.16 | $-64.80 | $9,602.80 | ▲ $10,690.57 (+690.57) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 101 | $10.77 | $2.32 | $+154.97 | $10,688.25 | ▲ $10,688.25 (+688.25) | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 85 | $49.76 | $2.25 | — | $6,456.41 | ▲ $10,686.01 (+686.01) | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+10.6; leftover $4275.30 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 22 | $41.31 | $2.06 | — | $5,545.53 | ▲ $10,683.95 (+683.95) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $916.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 280 | $3.27 | $3.61 | — | $4,626.32 | ▲ $10,680.34 (+680.34) | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+13.8; leftover $916.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 118 | $7.70 | $2.34 | — | $3,715.38 | ▲ $10,678.00 (+678.00) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $916.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 7 | $125.94 | $2.01 | — | $2,831.78 | ▲ $10,675.98 (+675.98) | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $916.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 750 | $1.22 | $9.68 | — | $1,907.11 | ▲ $10,666.31 (+666.31) | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $916.14 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 49 | $18.40 | $2.14 | — | $1,003.37 | ▲ $10,664.17 (+664.17) | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $916.14 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 58 | $15.70 | $2.16 | — | $90.61 | ▲ $10,662.01 (+662.01) | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $916.14 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 1 | $10.41 | $0.11 | — | $80.09 | ▲ $11,547.18 (+1,547.18) | 40% to #1, rest split; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $13.59 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 6 | $1.95 | $0.14 | — | $68.26 | ▲ $11,547.05 (+1,547.05) | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $13.59 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-14 | `TLN` | cash | leftover split 64.23 < 1 share @ 359.83 |
| 2026-08-14 | `VST` | cash | leftover split 13.76 < 1 share @ 146.90 |
| 2026-08-14 | `NRG` | cash | leftover split 13.76 < 1 share @ 120.00 |
| 2026-08-14 | `DAVE` | cash | leftover split 13.76 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 13.76 < 1 share @ 57.61 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TGTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `SLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `HIMS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EOG` | cash | leftover split 10.67 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 10.67 < 1 share @ 202.70 |
| 2026-08-17 | `ELF` | cash | leftover split 10.67 < 1 share @ 90.54 |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HNST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HNST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MUR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRMD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TBPH` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 42.62 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 9.13 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 9.13 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.13 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 9.13 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OCUL` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRMD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
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
| 2026-08-27 | `CRK` | cash | leftover split 11.28 < 1 share @ 14.09 |
| 2026-08-27 | `ACMR` | cash | leftover split 11.28 < 1 share @ 80.97 |
| 2026-08-27 | `MT` | cash | leftover split 11.28 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 11.28 < 1 share @ 925.74 |
| 2026-08-28 | `GGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-02 | `PBR-A` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 36.24 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 13.59 < 1 share @ 30.65 |
| 2026-09-04 | `BVS` | cash | leftover split 13.59 < 1 share @ 14.50 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 85 | 2026-09-03 @ $49.76 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+10.6; leftover $4275.30 |
| `HRMY` | 22 | 2026-09-03 @ $41.31 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $916.14 |
| `CABA` | 280 | 2026-09-03 @ $3.27 | 40% to #1, rest split; list flatten; 🔵; ⚪; ret5=+13.8; leftover $916.14 |
| `VSTM` | 118 | 2026-09-03 @ $7.70 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $916.14 |
| `RVTY` | 7 | 2026-09-03 @ $125.94 | 40% to #1, rest split; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $916.14 |
| `GPRO` | 750 | 2026-09-03 @ $1.22 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $916.14 |
| `FRVO` | 49 | 2026-09-03 @ $18.40 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $916.14 |
| `CRK` | 58 | 2026-09-03 @ $15.70 | 40% to #1, rest split; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $916.14 |
| `NVAX` | 1 | 2026-09-04 @ $10.41 | 40% to #1, rest split; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $13.59 |
| `BAK` | 6 | 2026-09-04 @ $1.95 | 40% to #1, rest split; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $13.59 |
