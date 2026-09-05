# Factor mine action — `union_last_green_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_green hold 5, no 🚨

Cash book **+19.41%** ($11,941) · signal-only (no cash/fees) was +52.19%. Starts YES **12/17**. Fills 83 · skips 216 · realized $+1545.07.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $69.56.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, INO, TNDM | — | $56.25 | $10,230.60 | $10,286.85 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | BUY BTSG x33 @ 59.80; BUY IREN x43 @ 45.98; BUY TPG x39 @ 50.62; BUY INO x2469 @ 0.81; BUY TNDM x85 @ 23.33 |
| 2026-08-14 | +5.50 | $56.25 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | LDI, BTBT, ANGX, HYLN | — | $34.95 | $10,642.58 | $10,677.53 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | BUY LDI x7 @ 0.94; BUY BTBT x4 @ 1.50; BUY ANGX x1 @ 4.31; BUY HYLN x1 @ 4.18 |
| 2026-08-17 | +2.25 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | — | — | $34.95 | $10,694.62 | $10,729.57 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | hold BTSG,IREN,TPG,INO,TNDM,LDI,BTBT,ANGX,HYLN |
| 2026-08-18 | -6.20 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | — | — | $34.95 | $10,798.65 | $10,833.60 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | — | — | $34.95 | $11,097.83 | $11,132.78 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | BTSG, IREN, TPG, INO, TNDM | $93.94 | $11,075.28 | $11,169.22 | LDI×7, BTBT×4, ANGX×1, HYLN×1, AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279 | SELL BTSG (dropped from list after 5 sess (min 5)); SELL IREN (dropped from list after 5 sess (min 5)); SELL TPG (dropped from list after 5 sess (min 5)); SELL INO (dropped from list after 5 sess (min 5)); SELL TNDM (dropped from list after 5 sess (min 5)); BUY AG x66 @ 20.55; BUY CDE x66 @ 20.65; BUY HDSN x238 @ 5.77; BUY IAG x70 @ 19.63; BUY KGC x46 @ 29.63; BUY NFGC x785 @ 1.75; BUY WPM x9 @ 144.54; BUY ABUS x279 @ 4.92 |
| 2026-08-21 | +3.25 | $93.94 | LDI×7, BTBT×4, ANGX×1, HYLN×1, AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279 | ARCT, CYPH, QDEL | LDI, ANGX, HYLN | $66.58 | $11,470.60 | $11,537.18 | BTBT×4, AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1 | SELL LDI (dropped from list after 5 sess (min 5)); SELL ANGX (dropped from list after 5 sess (min 5)); SELL HYLN (dropped from list after 5 sess (min 5)); BUY ARCT x1 @ 11.13; BUY CYPH x11 @ 1.32; BUY QDEL x1 @ 14.96 |
| 2026-08-24 | -5.17 | $66.58 | BTBT×4, AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1 | — | BTBT | $72.69 | $11,434.90 | $11,507.59 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1 | SELL BTBT (dropped from list after 6 sess (min 5)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $72.69 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1 | RZLT, NPWR, ALVO, ZURA | — | $47.59 | $11,477.67 | $11,525.26 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1 | BUY RZLT x1 @ 5.23; BUY NPWR x4 @ 2.00; BUY ALVO x1 @ 5.22; BUY ZURA x1 @ 6.38 |
| 2026-08-26 | +2.02 | $47.59 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1 | — | — | $47.59 | $11,560.73 | $11,608.32 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1 | hold AG,CDE,HDSN,IAG,KGC,NFGC,WPM,ABUS,ARCT,CYPH,QDEL,RZLT,NPWR,ALVO,ZURA |
| 2026-08-27 | — | $47.59 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1 | RRC, CRK, MOS, SLI, ANET, DLO, GEN | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $1,588.76 | $10,173.63 | $11,762.39 | ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50 | SELL AG (dropped from list after 5 sess (min 5)); SELL CDE (dropped from list after 5 sess (min 5)); SELL HDSN (dropped from list after 5 sess (min 5)); SELL IAG (dropped from list after 5 sess (min 5)); SELL KGC (dropped from list after 5 sess (min 5)); SELL NFGC (dropped from list after 5 sess (min 5)); SELL WPM (dropped from list after 5 sess (min 5)); SELL ABUS (dropped from list after 5 sess (min 5)); BUY RRC x35 @ 40.72; BUY CRK x102 @ 14.09; BUY MOS x58 @ 24.84; BUY SLI x558 @ 2.59; BUY ANET x7 @ 190.90; BUY DLO x92 @ 15.60; BUY GEN x50 @ 28.89 |
| 2026-08-28 | +0.75 | $1,588.76 | ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50 | ANF, BHVN, BZ, LVWR | ARCT, CYPH, QDEL | $116.46 | $11,625.56 | $11,742.02 | RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | SELL ARCT (dropped from list after 5 sess (min 5)); SELL CYPH (dropped from list after 5 sess (min 5)); SELL QDEL (dropped from list after 5 sess (min 5)); BUY ANF x2 @ 144.70; BUY BHVN x24 @ 16.95; BUY BZ x22 @ 18.50; BUY LVWR x296 @ 1.38 |
| 2026-08-31 | -5.85 | $116.46 | RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | — | — | $116.46 | $11,505.47 | $11,621.93 | RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $116.46 | RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | — | RZLT, NPWR, ALVO, ZURA | $138.78 | $11,496.42 | $11,635.20 | RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | SELL RZLT (dropped from list after 5 sess (min 5)); SELL NPWR (dropped from list after 5 sess (min 5)); SELL ALVO (dropped from list after 5 sess (min 5)); SELL ZURA (dropped from list after 5 sess (min 5)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $138.78 | RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | — | — | $138.78 | $11,517.54 | $11,656.32 | RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $138.78 | RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | ATRC, HRMY, VSTM, RVTY, GPRO, MMED, SLN | RRC, MOS, SLI, ANET, DLO, GEN | $166.91 | $12,116.37 | $12,283.28 | CRK×102, ANF×2, BHVN×24, BZ×22, LVWR×296, ATRC×24, HRMY×29, VSTM×159, RVTY×9, GPRO×1006, MMED×53, SLN×83 | SELL RRC (dropped from list after 5 sess (min 5)); SELL MOS (dropped from list after 5 sess (min 5)); SELL SLI (dropped from list after 5 sess (min 5)); SELL ANET (dropped from list after 5 sess (min 5)); SELL DLO (dropped from list after 5 sess (min 5)); SELL GEN (dropped from list after 5 sess (min 5)); BUY ATRC x24 @ 49.76; BUY HRMY x29 @ 41.31; BUY VSTM x159 @ 7.70; BUY RVTY x9 @ 125.94; BUY GPRO x1006 @ 1.22; BUY MMED x53 @ 22.78; BUY SLN x83 @ 14.70 |
| 2026-09-04 | — | $166.91 | CRK×102, ANF×2, BHVN×24, BZ×22, LVWR×296, ATRC×24, HRMY×29, VSTM×159, RVTY×9, GPRO×1006, MMED×53, SLN×83 | OSCR, NVAX, BVS, BAK, EOSE, DELL | CRK, ANF, BHVN, BZ, LVWR | $69.56 | $11,871.93 | $11,941.49 | ATRC×24, HRMY×29, VSTM×159, RVTY×9, GPRO×1006, MMED×53, SLN×83, OSCR×16, NVAX×49, BVS×35, BAK×266, EOSE×145, DELL×1 | SELL CRK (dropped from list after 6 sess (min 5)); SELL ANF (dropped from list after 5 sess (min 5)); SELL BHVN (dropped from list after 5 sess (min 5)); SELL BZ (dropped from list after 5 sess (min 5)); SELL LVWR (dropped from list after 5 sess (min 5)); BUY OSCR x16 @ 30.65; BUY NVAX x49 @ 10.41; BUY BVS x35 @ 14.50; BUY BAK x266 @ 1.95; BUY EOSE x145 @ 3.57; BUY DELL x1 @ 486.31 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | ▼ $9,997.91 (-2.09) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | ▼ $9,995.79 (-4.21) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | ▼ $9,993.69 (-6.31) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | ▼ $9,966.28 (-33.72) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | ▼ $9,964.03 (-35.97) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $49.60 | ▲ $10,321.16 (+321.16) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $43.53 | ▲ $10,321.09 (+321.09) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $39.18 | ▲ $10,321.04 (+321.04) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $34.95 | ▲ $10,321.00 (+321.00) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 33 | $58.64 | $2.11 | $-42.48 | $1,967.96 | ▲ $11,057.22 (+1,057.22) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 43 | $42.46 | $2.14 | $-155.62 | $3,791.59 | ▲ $11,055.08 (+1,055.08) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 39 | $53.06 | $2.13 | $+90.79 | $5,858.80 | ▲ $11,052.95 (+1,052.95) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 2469 | $1.30 | $32.29 | $+1150.12 | $9,036.21 | ▲ $11,020.66 (+1,020.66) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 85 | $23.11 | $2.27 | $-23.22 | $10,998.29 | ▲ $11,018.39 (+1,018.39) | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,639.80 | ▲ $11,016.20 (+1,016.20) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 66 | $20.65 | $2.19 | — | $8,274.71 | ▲ $11,014.01 (+1,014.01) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 238 | $5.77 | $3.07 | — | $6,898.38 | ▲ $11,010.94 (+1,010.94) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 70 | $19.63 | $2.20 | — | $5,522.08 | ▲ $11,008.74 (+1,008.74) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 46 | $29.63 | $2.13 | — | $4,156.97 | ▲ $11,006.61 (+1,006.61) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 785 | $1.75 | $10.13 | — | $2,773.10 | ▲ $10,996.48 (+996.48) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $1,470.22 | ▲ $10,994.47 (+994.47) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 279 | $4.92 | $3.60 | — | $93.94 | ▲ $10,990.87 (+990.87) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 7 | $0.87 | $0.10 | $-0.68 | $99.91 | ▲ $11,554.73 (+1,554.73) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 1 | $4.43 | $0.07 | $+0.01 | $104.27 | ▲ $11,554.66 (+1,554.66) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 1 | $3.42 | $0.06 | $-0.86 | $107.63 | ▲ $11,554.60 (+1,554.60) | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $96.39 | ▲ $11,554.49 (+1,554.49) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $15.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 11 | $1.32 | $0.18 | — | $81.69 | ▲ $11,554.31 (+1,554.31) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $15.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 1 | $14.96 | $0.15 | — | $66.58 | ▲ $11,554.16 (+1,554.16) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-1.6; leftover $15.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 4 | $1.55 | $0.09 | $+0.03 | $72.69 | ▲ $11,659.61 (+1,659.61) | dropped from list after 6 sess (min 5) | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 1 | $5.23 | $0.06 | — | $67.40 | ▲ $11,608.53 (+1,608.53) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+10.7; leftover $9.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 4 | $2.00 | $0.09 | — | $59.31 | ▲ $11,608.44 (+1,608.44) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $9.09 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 1 | $5.22 | $0.06 | — | $54.03 | ▲ $11,608.38 (+1,608.38) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $9.09 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 1 | $6.38 | $0.07 | — | $47.59 | ▲ $11,608.32 (+1,608.32) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $9.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.63 | $2.21 | $+0.88 | $1,406.96 | ▲ $11,670.80 (+1,670.80) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 66 | $21.00 | $2.21 | $+18.70 | $2,790.75 | ▲ $11,668.59 (+1,668.59) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 238 | $5.51 | $3.12 | $-68.07 | $4,099.01 | ▲ $11,665.47 (+1,665.47) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 70 | $21.64 | $2.22 | $+136.28 | $5,611.58 | ▲ $11,663.24 (+1,663.24) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 46 | $32.90 | $2.15 | $+146.14 | $7,122.83 | ▲ $11,661.09 (+1,661.09) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 785 | $2.00 | $10.27 | $+175.85 | $8,682.56 | ▲ $11,650.82 (+1,650.82) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $10,128.89 | ▲ $11,648.78 (+1,648.78) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABUS` | 279 | $5.19 | $3.66 | $+68.07 | $11,573.25 | ▲ $11,645.13 (+1,645.13) | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 35 | $40.72 | $2.10 | — | $10,145.95 | ▲ $11,643.03 (+1,643.03) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+1.8; leftover $1446.66 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 102 | $14.09 | $2.30 | — | $8,706.48 | ▲ $11,640.74 (+1,640.74) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+1.1; leftover $1446.66 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 58 | $24.84 | $2.16 | — | $7,263.59 | ▲ $11,638.57 (+1,638.57) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+13.0; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 558 | $2.59 | $7.20 | — | $5,811.17 | ▲ $11,631.37 (+1,631.37) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+4.2; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 7 | $190.90 | $2.01 | — | $4,472.86 | ▲ $11,629.36 (+1,629.36) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=-5.1; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 92 | $15.60 | $2.27 | — | $3,035.40 | ▲ $11,627.10 (+1,627.10) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+7.1; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 50 | $28.89 | $2.14 | — | $1,588.76 | ▲ $11,624.96 (+1,624.96) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+1.6; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.74 | $0.18 | $+4.32 | $1,604.32 | ▲ $11,768.98 (+1,768.98) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 11 | $1.75 | $0.25 | $+4.31 | $1,623.32 | ▲ $11,768.73 (+1,768.73) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QDEL` | 1 | $14.92 | $0.17 | $-0.36 | $1,638.07 | ▲ $11,768.56 (+1,768.56) | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 2 | $144.70 | $2.00 | — | $1,346.67 | ▲ $11,766.56 (+1,766.56) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $409.52 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 24 | $16.95 | $2.06 | — | $937.81 | ▲ $11,764.50 (+1,764.50) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $409.52 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 22 | $18.50 | $2.06 | — | $528.76 | ▲ $11,762.45 (+1,762.45) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $409.52 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 296 | $1.38 | $3.82 | — | $116.46 | ▲ $11,758.63 (+1,758.63) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $409.52 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 1 | $4.69 | $0.07 | $-0.67 | $121.08 | ▲ $11,615.62 (+1,615.62) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 4 | $1.78 | $0.10 | $-1.08 | $128.09 | ▲ $11,615.51 (+1,615.51) | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 1 | $5.24 | $0.08 | $-0.11 | $133.26 | ▲ $11,615.44 (+1,615.44) | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 1 | $5.60 | $0.08 | $-0.93 | $138.78 | ▲ $11,615.36 (+1,615.36) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 35 | $42.10 | $2.12 | $+44.09 | $1,610.16 | ▲ $11,612.22 (+1,612.22) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 58 | $24.70 | $2.19 | $-12.47 | $3,040.58 | ▲ $11,610.04 (+1,610.04) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 558 | $2.49 | $7.30 | $-70.30 | $4,422.70 | ▲ $11,602.74 (+1,602.74) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ANET` | 7 | $188.00 | $2.03 | $-24.34 | $5,736.66 | ▲ $11,600.70 (+1,600.70) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `DLO` | 92 | $14.82 | $2.29 | $-76.32 | $7,097.81 | ▲ $11,598.41 (+1,598.41) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GEN` | 50 | $30.04 | $2.16 | $+53.20 | $8,597.65 | ▲ $11,596.25 (+1,596.25) | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $7,401.35 | ▲ $11,594.19 (+1,594.19) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $6,201.28 | ▲ $11,592.11 (+1,592.11) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 159 | $7.70 | $2.47 | — | $4,974.51 | ▲ $11,589.64 (+1,589.64) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,839.04 | ▲ $11,587.63 (+1,587.63) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1006 | $1.22 | $12.98 | — | $2,598.74 | ▲ $11,574.65 (+1,574.65) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1228.24 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $1,389.25 | ▲ $11,572.50 (+1,572.50) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 83 | $14.70 | $2.24 | — | $166.91 | ▲ $11,570.26 (+1,570.26) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 102 | $15.45 | $2.33 | $+134.10 | $1,740.49 | ▲ $12,396.17 (+2,396.17) | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 2 | $137.70 | $2.02 | $-18.01 | $2,013.87 | ▲ $12,394.15 (+2,394.15) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 24 | $15.89 | $2.08 | $-29.58 | $2,393.15 | ▲ $12,392.07 (+2,392.07) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 22 | $17.31 | $2.08 | $-30.31 | $2,771.89 | ▲ $12,389.99 (+2,389.99) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LVWR` | 296 | $1.17 | $3.88 | $-69.86 | $3,114.33 | ▲ $12,386.11 (+2,386.11) | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 16 | $30.65 | $2.04 | — | $2,621.90 | ▲ $12,384.08 (+2,384.08) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ret5=-2.2; leftover $519.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 49 | $10.41 | $2.14 | — | $2,109.67 | ▲ $12,381.94 (+2,381.94) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $519.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 35 | $14.50 | $2.10 | — | $1,600.07 | ▲ $12,379.84 (+2,379.84) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $519.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 266 | $1.95 | $3.43 | — | $1,077.94 | ▲ $12,376.41 (+2,376.41) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $519.06 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 145 | $3.57 | $2.42 | — | $557.87 | ▲ $12,373.99 (+2,373.99) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $519.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $486.31 | $1.99 | — | $69.56 | ▲ $12,371.99 (+2,371.99) | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $519.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-14 | `VST` | cash | leftover split 7.03 < 1 share @ 146.90 |
| 2026-08-14 | `DAVE` | cash | leftover split 7.03 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 7.03 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 7.03 < 1 share @ 14.80 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.37 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 4.37 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 4.37 < 1 share @ 202.70 |
| 2026-08-17 | `NB` | cash | leftover split 4.37 < 1 share @ 5.07 |
| 2026-08-17 | `CDNL` | cash | leftover split 4.37 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 4.37 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 4.37 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 4.37 < 1 share @ 92.99 |
| 2026-08-18 | `BTSG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `IREN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TPG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `TNDM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `TNDM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-19 | `LDI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `LDI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 15.38 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 15.38 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 15.38 < 1 share @ 216.30 |
| 2026-08-21 | `DE` | cash | leftover split 15.38 < 1 share @ 623.26 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `CDE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `HDSN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `IAG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `KGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NFGC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `WPM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ABUS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MOS` | cash | leftover split 9.09 < 1 share @ 24.00 |
| 2026-08-25 | `INSP` | cash | leftover split 9.09 < 1 share @ 61.47 |
| 2026-08-25 | `HCA` | cash | leftover split 9.09 < 1 share @ 429.24 |
| 2026-08-25 | `ALIT` | cash | leftover split 9.09 < 1 share @ 14.86 |
| 2026-08-26 | `AG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `CDE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `HDSN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `IAG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `KGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NFGC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `WPM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ABUS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QDEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `HCA` | no_price | no 09:30 open |
| 2026-08-26 | `MOS` | no_price | no 09:30 open |
| 2026-08-26 | `INSP` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QDEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ASML` | cash | leftover split 1446.66 < 1 share @ 1746.33 |
| 2026-08-28 | `RZLT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ANET` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `RZLT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `MOS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ANET` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `MOS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ANET` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `DLO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `GEN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `RRC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `CRK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `MOS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `SLI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ANET` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `DLO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `GEN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `LVWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `LVWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 24 | 2026-09-03 @ $49.76 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1228.24 |
| `HRMY` | 29 | 2026-09-03 @ $41.31 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1228.24 |
| `VSTM` | 159 | 2026-09-03 @ $7.70 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1228.24 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1228.24 |
| `GPRO` | 1006 | 2026-09-03 @ $1.22 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1228.24 |
| `MMED` | 53 | 2026-09-03 @ $22.78 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1228.24 |
| `SLN` | 83 | 2026-09-03 @ $14.70 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1228.24 |
| `OSCR` | 16 | 2026-09-04 @ $30.65 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ret5=-2.2; leftover $519.06 |
| `NVAX` | 49 | 2026-09-04 @ $10.41 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $519.06 |
| `BVS` | 35 | 2026-09-04 @ $14.50 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $519.06 |
| `BAK` | 266 | 2026-09-04 @ $1.95 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $519.06 |
| `EOSE` | 145 | 2026-09-04 @ $3.57 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $519.06 |
| `DELL` | 1 | 2026-09-04 @ $486.31 | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $519.06 |
