# Factor mine action — `union_last_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ last_green, no 🚨

Cash book **+13.14%** ($11,314) · signal-only (no cash/fees) was +15.19%. Starts YES **16/17**. Fills 130 · skips 57 · realized $+1017.55.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $414.63.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | BTSG, IREN, TPG, INO, TNDM | — | $56.25 | $10,230.60 | $10,286.85 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | BUY BTSG x33 @ 59.80; BUY IREN x43 @ 45.98; BUY TPG x39 @ 50.62; BUY INO x2469 @ 0.81; BUY TNDM x85 @ 23.33 |
| 2026-08-14 | +5.50 | $56.25 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | VST, DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN | BTSG, IREN, TPG, INO, TNDM | $393.24 | $9,725.90 | $10,119.14 | VST×8, DAVE×3, SLG×22, LDI×1371, BTBT×856, BETR×86, ANGX×298, HYLN×307 | SELL BTSG (dropped from list after 1 sess (min 1)); SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL INO (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); BUY VST x8 @ 146.90; BUY DAVE x3 @ 330.91; BUY SLG x22 @ 57.61; BUY LDI x1371 @ 0.94; BUY BTBT x856 @ 1.50; BUY BETR x86 @ 14.80; BUY ANGX x298 @ 4.31; BUY HYLN x307 @ 4.18 |
| 2026-08-17 | +2.25 | $393.24 | VST×8, DAVE×3, SLG×22, LDI×1371, BTBT×856, BETR×86, ANGX×298, HYLN×307 | DVN, EOG, FANG, NB, CDNL, ABX, VERA, CELC | VST, DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN | $282.23 | $9,830.63 | $10,112.86 | DVN×27, EOG×8, FANG×6, NB×249, CDNL×31, ABX×138, VERA×40, CELC×13 | SELL VST (dropped from list after 1 sess (min 1)); SELL DAVE (dropped from list after 1 sess (min 1)); SELL SLG (dropped from list after 1 sess (min 1)); SELL LDI (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL BETR (dropped from list after 1 sess (min 1)); SELL ANGX (dropped from list after 1 sess (min 1)); SELL HYLN (dropped from list after 1 sess (min 1)); BUY DVN x27 @ 46.18; BUY EOG x8 @ 142.77; BUY FANG x6 @ 202.70; BUY NB x249 @ 5.07; BUY CDNL x31 @ 39.85; BUY ABX x138 @ 9.12; BUY VERA x40 @ 31.30; BUY CELC x13 @ 92.99 |
| 2026-08-18 | -6.20 | $282.23 | DVN×27, EOG×8, FANG×6, NB×249, CDNL×31, ABX×138, VERA×40, CELC×13 | — | DVN, EOG, FANG, NB, CDNL, ABX, VERA, CELC | $10,146.49 | $0.00 | $10,146.49 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL EOG (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL NB (dropped from list after 1 sess (min 1)); SELL CDNL (dropped from list after 1 sess (min 1)); SELL ABX (dropped from list after 1 sess (min 1)); SELL VERA (dropped from list after 1 sess (min 1)); SELL CELC (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $10,146.49 | — | — | — | $10,146.49 | $0.00 | $10,146.49 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $10,146.49 | — | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $154.98 | $10,127.08 | $10,282.06 | AG×61, CDE×61, HDSN×219, IAG×64, KGC×42, NFGC×724, WPM×8, ABUS×257 | BUY AG x61 @ 20.55; BUY CDE x61 @ 20.65; BUY HDSN x219 @ 5.77; BUY IAG x64 @ 19.63; BUY KGC x42 @ 29.63; BUY NFGC x724 @ 1.75; BUY WPM x8 @ 144.54; BUY ABUS x257 @ 4.92 |
| 2026-08-21 | +3.25 | $154.98 | AG×61, CDE×61, HDSN×219, IAG×64, KGC×42, NFGC×724, WPM×8, ABUS×257 | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, QDEL | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $99.41 | $10,751.06 | $10,850.47 | AU×11, AUPH×77, AEM×6, ARCT×119, CYPH×1004, BTBT×798, DE×2, QDEL×88 | SELL AG (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL HDSN (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); SELL ABUS (dropped from list after 1 sess (min 1)); BUY AU x11 @ 119.43; BUY AUPH x77 @ 17.20; BUY AEM x6 @ 216.30; BUY ARCT x119 @ 11.13; BUY CYPH x1004 @ 1.32; BUY BTBT x798 @ 1.66; BUY DE x2 @ 623.26; BUY QDEL x88 @ 14.96 |
| 2026-08-24 | -5.17 | $99.41 | AU×11, AUPH×77, AEM×6, ARCT×119, CYPH×1004, BTBT×798, DE×2, QDEL×88 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, QDEL | $11,222.61 | $0.00 | $11,222.61 | — | SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL AEM (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); SELL DE (dropped from list after 1 sess (min 1)); SELL QDEL (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $11,222.61 | — | MOS, INSP, RZLT, HCA, NPWR, ALVO, ALIT, ZURA | — | $166.62 | $11,077.36 | $11,243.98 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | BUY MOS x58 @ 24.00; BUY INSP x22 @ 61.47; BUY RZLT x268 @ 5.23; BUY HCA x3 @ 429.24; BUY NPWR x701 @ 2.00; BUY ALVO x268 @ 5.22; BUY ALIT x94 @ 14.86; BUY ZURA x219 @ 6.38 |
| 2026-08-26 | +2.02 | $166.62 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | — | — | $166.62 | $11,028.72 | $11,195.34 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | hold MOS,INSP,RZLT,HCA,NPWR,ALVO,ALIT,ZURA |
| 2026-08-27 | — | $166.62 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | RRC, CRK, SLI, ANET, DLO, GEN | INSP, RZLT, HCA, NPWR, ALVO, ALIT, ZURA | $1,391.41 | $9,677.22 | $11,068.63 | MOS×58, RRC×33, CRK×96, SLI×524, ANET×7, DLO×87, GEN×47 | SELL INSP (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); SELL NPWR (dropped from list after 2 sess (min 1)); SELL ALVO (dropped from list after 2 sess (min 1)); SELL ALIT (dropped from list after 2 sess (min 1)); SELL ZURA (dropped from list after 2 sess (min 1)); BUY RRC x33 @ 40.72; BUY CRK x96 @ 14.09; BUY SLI x524 @ 2.59; BUY ANET x7 @ 190.90; BUY DLO x87 @ 15.60; BUY GEN x47 @ 28.89 |
| 2026-08-28 | +0.75 | $1,391.41 | MOS×58, RRC×33, CRK×96, SLI×524, ANET×7, DLO×87, GEN×47 | ANF, BHVN, BZ, LVWR | ANET, DLO, GEN | $73.15 | $10,892.19 | $10,965.34 | MOS×58, RRC×33, CRK×96, SLI×524, ANF×9, BHVN×82, BZ×75, LVWR×1007 | SELL ANET (dropped from list after 1 sess (min 1)); SELL DLO (dropped from list after 1 sess (min 1)); SELL GEN (dropped from list after 1 sess (min 1)); BUY ANF x9 @ 144.70; BUY BHVN x82 @ 16.95; BUY BZ x75 @ 18.50; BUY LVWR x1007 @ 1.38 |
| 2026-08-31 | -5.85 | $73.15 | MOS×58, RRC×33, CRK×96, SLI×524, ANF×9, BHVN×82, BZ×75, LVWR×1007 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, LVWR | $10,812.57 | $0.00 | $10,812.57 | — | SELL MOS (dropped from list after 4 sess (min 1)); SELL RRC (dropped from list after 2 sess (min 1)); SELL CRK (dropped from list after 2 sess (min 1)); SELL SLI (dropped from list after 2 sess (min 1)); SELL ANF (dropped from list after 1 sess (min 1)); SELL BHVN (dropped from list after 1 sess (min 1)); SELL BZ (dropped from list after 1 sess (min 1)); SELL LVWR (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,812.57 | — | — | — | $10,812.57 | $0.00 | $10,812.57 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,812.57 | — | — | — | $10,812.57 | $0.00 | $10,812.57 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,812.57 | — | ATRC, HRMY, VSTM, RVTY, GPRO, CRK, MMED, SLN | — | $128.12 | $11,459.35 | $11,587.47 | ATRC×27, HRMY×32, VSTM×175, RVTY×10, GPRO×1107, CRK×86, MMED×59, SLN×91 | BUY ATRC x27 @ 49.76; BUY HRMY x32 @ 41.31; BUY VSTM x175 @ 7.70; BUY RVTY x10 @ 125.94; BUY GPRO x1107 @ 1.22; BUY CRK x86 @ 15.70; BUY MMED x59 @ 22.78; BUY SLN x91 @ 14.70 |
| 2026-09-04 | — | $128.12 | ATRC×27, HRMY×32, VSTM×175, RVTY×10, GPRO×1107, CRK×86, MMED×59, SLN×91 | OSCR, NVAX, BVS, BAK, EOSE, DELL | HRMY, VSTM, RVTY, CRK, MMED, SLN | $414.63 | $10,899.71 | $11,314.34 | ATRC×27, GPRO×1107, OSCR×45, NVAX×132, BVS×95, BAK×710, EOSE×387, DELL×2 | SELL HRMY (dropped from list after 1 sess (min 1)); SELL VSTM (dropped from list after 1 sess (min 1)); SELL RVTY (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL SLN (dropped from list after 1 sess (min 1)); BUY OSCR x45 @ 30.65; BUY NVAX x132 @ 10.41; BUY BVS x95 @ 14.50; BUY BAK x710 @ 1.95; BUY EOSE x387 @ 3.57; BUY DELL x2 @ 486.31 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 33 | $59.65 | $2.11 | $-9.15 | $2,022.58 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 43 | $44.09 | $2.14 | $-85.53 | $3,916.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 39 | $55.29 | $2.13 | $+177.76 | $6,070.49 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 2469 | $0.93 | $30.80 | $+238.08 | $8,335.86 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 85 | $22.92 | $2.27 | $-39.37 | $10,281.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $9,104.57 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+3.6; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $8,109.84 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $6,840.37 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+5.7; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1371 | $0.94 | $16.96 | — | $5,538.78 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 856 | $1.50 | $11.04 | — | $4,243.74 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $2,968.69 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 298 | $4.31 | $3.84 | — | $1,680.46 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 307 | $4.18 | $3.96 | — | $393.24 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $1,586.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $2,594.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $3,811.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1371 | $0.91 | $16.79 | $-74.87 | $5,037.75 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 856 | $1.52 | $11.19 | $-5.12 | $6,327.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $7,501.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 298 | $4.60 | $3.90 | $+78.67 | $8,867.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 307 | $4.10 | $4.02 | $-32.54 | $10,122.59 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,873.66 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+6.7; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,729.49 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+5.8; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,511.28 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+8.3; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 249 | $5.07 | $3.21 | — | $5,245.64 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=-4.7; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $4,008.21 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1265.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $2,747.24 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $1,493.13 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1265.32 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $282.23 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.8; leftover $1265.32 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,576.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,758.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $4,009.98 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 249 | $4.66 | $3.26 | $-108.57 | $5,167.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $6,453.62 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $7,697.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 40 | $31.31 | $2.13 | $-3.84 | $8,947.60 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $10,146.49 | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,890.76 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $7,628.94 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 219 | $5.77 | $2.83 | — | $6,362.49 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,103.98 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,857.41 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 724 | $1.75 | $9.34 | — | $2,581.07 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,422.73 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 257 | $4.92 | $3.32 | — | $154.98 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,488.68 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $2,813.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 219 | $5.67 | $2.87 | $-27.60 | $4,052.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $5,404.78 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $6,753.78 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 724 | $1.79 | $9.47 | $+10.15 | $8,040.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,275.84 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 257 | $5.20 | $3.37 | $+65.28 | $10,608.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,293.11 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 77 | $17.20 | $2.22 | — | $7,966.49 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,666.68 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 119 | $11.13 | $2.35 | — | $5,339.87 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1004 | $1.32 | $12.95 | — | $4,001.64 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 798 | $1.66 | $10.29 | — | $2,666.66 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $1,418.15 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1326.11 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 88 | $14.96 | $2.25 | — | $99.41 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-1.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.50 | $2.04 | $+7.70 | $1,422.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 77 | $16.60 | $2.24 | $-50.66 | $2,698.82 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,998.98 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 119 | $13.26 | $2.38 | $+248.74 | $5,574.54 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1004 | $1.83 | $13.13 | $+485.96 | $7,398.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 798 | $1.55 | $10.44 | $-108.51 | $8,625.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $9,930.41 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 88 | $14.71 | $2.28 | $-26.53 | $11,222.61 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $24.00 | $2.16 | — | $9,828.45 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.0; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $8,474.05 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+9.2; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 268 | $5.23 | $3.46 | — | $7,068.95 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+10.7; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $5,779.23 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+6.1; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 701 | $2.00 | $9.04 | — | $4,368.19 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1402.83 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 268 | $5.22 | $3.46 | — | $2,965.77 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1402.83 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 94 | $14.86 | $2.27 | — | $1,566.66 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1402.83 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 219 | $6.38 | $2.83 | — | $166.62 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $1,486.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 268 | $5.01 | $3.51 | $-65.93 | $2,825.25 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $4,105.73 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 701 | $1.93 | $9.17 | $-67.28 | $5,449.49 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 268 | $4.98 | $3.51 | $-71.29 | $6,780.62 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 94 | $14.85 | $2.30 | $-5.51 | $8,174.22 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 219 | $6.13 | $2.87 | $-60.45 | $9,513.82 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,167.97 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+1.8; leftover $1359.12 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.09 | $2.28 | — | $6,813.05 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+1.1; leftover $1359.12 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 524 | $2.59 | $6.76 | — | $5,449.13 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.2; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 7 | $190.90 | $2.01 | — | $4,110.82 | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=-5.1; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 87 | $15.60 | $2.25 | — | $2,751.37 | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+7.1; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 47 | $28.89 | $2.13 | — | $1,391.41 | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+1.6; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 7 | $205.90 | $2.03 | $+100.96 | $2,830.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 87 | $15.33 | $2.28 | $-28.02 | $4,162.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 47 | $29.83 | $2.15 | $+39.90 | $5,561.97 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,257.65 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1390.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 82 | $16.95 | $2.24 | — | $2,865.51 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1390.49 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 75 | $18.50 | $2.21 | — | $1,475.80 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1390.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1007 | $1.38 | $12.99 | — | $73.15 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1390.49 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.75 | $2.19 | $-18.85 | $1,448.46 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,802.98 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 96 | $14.56 | $2.31 | $+40.54 | $4,198.44 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 524 | $2.51 | $6.86 | $-55.54 | $5,506.82 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,842.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 82 | $15.44 | $2.26 | $-128.32 | $8,106.63 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 75 | $17.89 | $2.24 | $-50.20 | $9,446.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 1007 | $1.37 | $13.17 | $-36.23 | $10,812.57 | dropped from list after 1 sess (min 1) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,466.97 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,142.97 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 175 | $7.70 | $2.52 | — | $6,792.95 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $5,531.53 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1107 | $1.22 | $14.28 | — | $4,166.71 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1351.57 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.70 | $2.25 | — | $2,814.27 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1351.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $22.78 | $2.17 | — | $1,468.08 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 91 | $14.70 | $2.26 | — | $128.12 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,499.77 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 175 | $8.03 | $2.56 | $+52.68 | $2,902.46 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,224.92 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 86 | $15.45 | $2.27 | $-26.02 | $5,551.35 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 59 | $23.88 | $2.19 | $+60.54 | $6,958.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 91 | $14.85 | $2.29 | $+9.10 | $8,307.14 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 45 | $30.65 | $2.12 | — | $6,925.77 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=-2.2; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 132 | $10.41 | $2.39 | — | $5,549.26 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 95 | $14.50 | $2.27 | — | $4,169.49 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 710 | $1.95 | $9.16 | — | $2,775.83 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1384.52 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 387 | $3.57 | $4.99 | — | $1,389.25 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1384.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $414.63 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-9.9; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟢 buy🟡 |

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
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1359.12 < 1 share @ 1746.33 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 27 | 2026-09-03 @ $49.76 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1351.57 |
| `GPRO` | 1107 | 2026-09-03 @ $1.22 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1351.57 |
| `OSCR` | 45 | 2026-09-04 @ $30.65 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=-2.2; leftover $1384.52 |
| `NVAX` | 132 | 2026-09-04 @ $10.41 | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1384.52 |
| `BVS` | 95 | 2026-09-04 @ $14.50 | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1384.52 |
| `BAK` | 710 | 2026-09-04 @ $1.95 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1384.52 |
| `EOSE` | 387 | 2026-09-04 @ $3.57 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1384.52 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-9.9; leftover $1384.52 |
