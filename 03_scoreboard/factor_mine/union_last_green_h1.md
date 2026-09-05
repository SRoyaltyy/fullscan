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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, INO, TNDM | — | $56.25 | $10,286.85 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $56.25 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | $10,321.25 | +34.40 | VST, DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN | BTSG, IREN, TPG, INO, TNDM | $393.24 | $10,119.14 | VST×8, DAVE×3, SLG×22, LDI×1371, BTBT×856, BETR×86, ANGX×298, HYLN×307 | 09:30 open · cash $56.25 (unchanged overnight, no fees) · equity $10,321.25 vs prior close $10,286.85 (+34.40) because holdings re-marked: BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; INO×2469 yday $0.90 → 09:30 $0.93 +74.07; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85 |
| 2026-08-17 | +2.25 | $393.24 | VST×8, DAVE×3, SLG×22, LDI×1371, BTBT×856, BETR×86, ANGX×298, HYLN×307 | $10,166.90 | +47.76 | DVN, EOG, FANG, NB, CDNL, ABX, VERA, CELC | VST, DAVE, SLG, LDI, BTBT, BETR, ANGX, HYLN | $282.23 | $10,112.86 | DVN×27, EOG×8, FANG×6, NB×249, CDNL×31, ABX×138, VERA×40, CELC×13 | 09:30 open · cash $393.24 (unchanged overnight, no fees) · equity $10,166.90 vs prior close $10,119.14 (+47.76) because holdings re-marked: VST×8 yday $148.13 → 09:30 $149.37 +9.92; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; LDI×1371 yday $0.90 → 09:30 $0.91 +13.71; BTBT×856 yday $1.57 → 09:30 $1.52 -42.80; BETR×86 yday $13.73 → 09:30 $13.67 -5.16; ANGX×298 yday $4.37 → 09:30 $4.60 +68.54; HYLN×307 yday $4.06 → 09:30 $4.10 +12.28 |
| 2026-08-18 | -6.20 | $282.23 | DVN×27, EOG×8, FANG×6, NB×249, CDNL×31, ABX×138, VERA×40, CELC×13 | $10,164.62 | +51.76 | — | DVN, EOG, FANG, NB, CDNL, ABX, VERA, CELC | $10,146.49 | $10,146.49 | — | 09:30 open · cash $282.23 (unchanged overnight, no fees) · equity $10,164.62 vs prior close $10,112.86 (+51.76) because holdings re-marked: DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; NB×249 yday $4.81 → 09:30 $4.66 -37.35; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×138 yday $9.12 → 09:30 $9.03 -12.42; VERA×40 yday $31.63 → 09:30 $31.31 -12.80; CELC×13 yday $92.44 → 09:30 $92.38 -0.78 |
| 2026-08-19 | -7.20 | $10,146.49 | — | $10,146.49 | -0.00 | — | — | $10,146.49 | $10,146.49 | — | 09:30 open · cash $10,146.49 · no holdings · equity $10,146.49 vs prior close $10,146.49 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $10,146.49 | — | $10,146.49 | -0.00 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $154.98 | $10,282.06 | AG×61, CDE×61, HDSN×219, IAG×64, KGC×42, NFGC×724, WPM×8, ABUS×257 | 09:30 open · cash $10,146.49 · no holdings · equity $10,146.49 vs prior close $10,146.49 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $154.98 | AG×61, CDE×61, HDSN×219, IAG×64, KGC×42, NFGC×724, WPM×8, ABUS×257 | $10,635.34 | +353.28 | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, QDEL | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $99.41 | $10,850.47 | AU×11, AUPH×77, AEM×6, ARCT×119, CYPH×1004, BTBT×798, DE×2, QDEL×88 | 09:30 open · cash $154.98 (unchanged overnight, no fees) · equity $10,635.34 vs prior close $10,282.06 (+353.28) because holdings re-marked: AG×61 yday $21.19 → 09:30 $21.90 +43.31; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×219 yday $5.57 → 09:30 $5.67 +21.90; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×724 yday $1.75 → 09:30 $1.79 +28.96; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×257 yday $4.77 → 09:30 $5.20 +110.51 |
| 2026-08-24 | -5.17 | $99.41 | AU×11, AUPH×77, AEM×6, ARCT×119, CYPH×1004, BTBT×798, DE×2, QDEL×88 | $11,259.17 | +408.70 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, QDEL | $11,222.61 | $11,222.61 | — | 09:30 open · cash $99.41 (unchanged overnight, no fees) · equity $11,259.17 vs prior close $10,850.47 (+408.70) because holdings re-marked: AU×11 yday $121.22 → 09:30 $120.50 -7.92; AUPH×77 yday $16.65 → 09:30 $16.60 -3.85; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×119 yday $13.45 → 09:30 $13.26 -22.61; CYPH×1004 yday $1.42 → 09:30 $1.83 +411.64; BTBT×798 yday $1.53 → 09:30 $1.55 +15.96; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×88 yday $14.74 → 09:30 $14.71 -2.64 |
| 2026-08-25 | +1.80 | $11,222.61 | — | $11,222.61 | +0.00 | MOS, INSP, RZLT, HCA, NPWR, ALVO, ALIT, ZURA | — | $166.62 | $11,243.98 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | 09:30 open · cash $11,222.61 · no holdings · equity $11,222.61 vs prior close $11,222.61 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $166.62 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | $11,243.98 | -0.00 | — | — | $166.62 | $11,195.34 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | 09:30 open · cash $166.62 (unchanged overnight, no fees) · equity $11,243.98 vs prior close $11,243.98 (-0.00) because holdings re-marked: MOS×58 yday $23.75 → 09:30 $23.75 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; RZLT×268 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; NPWR×701 yday $2.02 → 09:30 $2.02 +0.00; ALVO×268 yday $5.25 → 09:30 $5.25 +0.00; ALIT×94 yday $14.87 → 09:30 $14.87 +0.00; ZURA×219 yday $6.50 → 09:30 $6.50 +0.00 |
| 2026-08-27 | — | $166.62 | MOS×58, INSP×22, RZLT×268, HCA×3, NPWR×701, ALVO×268, ALIT×94, ZURA×219 | $10,980.00 | -215.34 | RRC, CRK, SLI, ANET, DLO, GEN | INSP, RZLT, HCA, NPWR, ALVO, ALIT, ZURA | $1,391.41 | $11,068.63 | MOS×58, RRC×33, CRK×96, SLI×524, ANET×7, DLO×87, GEN×47 | 09:30 open · cash $166.62 (unchanged overnight, no fees) · equity $10,980.00 vs prior close $11,195.34 (-215.34) because holdings re-marked: MOS×58 yday $23.75 → 09:30 $24.84 +63.22; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; RZLT×268 yday $5.29 → 09:30 $5.01 -75.04; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; NPWR×701 yday $2.02 → 09:30 $1.93 -63.09; ALVO×268 yday $5.25 → 09:30 $4.98 -72.36; ALIT×94 yday $14.87 → 09:30 $14.85 -1.88; ZURA×219 yday $6.50 → 09:30 $6.13 -81.03 |
| 2026-08-28 | +0.75 | $1,391.41 | MOS×58, RRC×33, CRK×96, SLI×524, ANET×7, DLO×87, GEN×47 | $11,074.67 | +6.04 | ANF, BHVN, BZ, LVWR | ANET, DLO, GEN | $73.15 | $10,965.34 | MOS×58, RRC×33, CRK×96, SLI×524, ANF×9, BHVN×82, BZ×75, LVWR×1007 | 09:30 open · cash $1,391.41 (unchanged overnight, no fees) · equity $11,074.67 vs prior close $11,068.63 (+6.04) because holdings re-marked: MOS×58 yday $24.16 → 09:30 $24.00 -9.28; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×96 yday $14.50 → 09:30 $14.42 -7.68; SLI×524 yday $2.61 → 09:30 $2.60 -5.24; ANET×7 yday $202.25 → 09:30 $205.90 +25.55; DLO×87 yday $15.36 → 09:30 $15.33 -2.61; GEN×47 yday $29.64 → 09:30 $29.83 +8.93 |
| 2026-08-31 | -5.85 | $73.15 | MOS×58, RRC×33, CRK×96, SLI×524, ANF×9, BHVN×82, BZ×75, LVWR×1007 | $10,845.73 | -119.61 | — | MOS, RRC, CRK, SLI, ANF, BHVN, BZ, LVWR | $10,812.57 | $10,812.57 | — | 09:30 open · cash $73.15 (unchanged overnight, no fees) · equity $10,845.73 vs prior close $10,965.34 (-119.61) because holdings re-marked: MOS×58 yday $23.76 → 09:30 $23.75 -0.58; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×96 yday $14.62 → 09:30 $14.56 -5.76; SLI×524 yday $2.64 → 09:30 $2.51 -68.12; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×82 yday $16.12 → 09:30 $15.44 -55.76; BZ×75 yday $18.00 → 09:30 $17.89 -8.25; LVWR×1007 yday $1.36 → 09:30 $1.37 +10.07 |
| 2026-09-01 | -6.30 | $10,812.57 | — | $10,812.57 | -0.00 | — | — | $10,812.57 | $10,812.57 | — | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,812.57 | — | $10,812.57 | -0.00 | — | — | $10,812.57 | $10,812.57 | — | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,812.57 | — | $10,812.57 | -0.00 | ATRC, HRMY, VSTM, RVTY, GPRO, CRK, MMED, SLN | — | $128.12 | $11,587.47 | ATRC×27, HRMY×32, VSTM×175, RVTY×10, GPRO×1107, CRK×86, MMED×59, SLN×91 | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $128.12 | ATRC×27, HRMY×32, VSTM×175, RVTY×10, GPRO×1107, CRK×86, MMED×59, SLN×91 | $11,718.82 | +131.35 | OSCR, NVAX, BVS, BAK, EOSE, DELL | HRMY, VSTM, RVTY, CRK, MMED, SLN | $414.63 | $11,314.34 | ATRC×27, GPRO×1107, OSCR×45, NVAX×132, BVS×95, BAK×710, EOSE×387, DELL×2 | 09:30 open · cash $128.12 (unchanged overnight, no fees) · equity $11,718.82 vs prior close $11,587.47 (+131.35) because holdings re-marked: ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; VSTM×175 yday $8.02 → 09:30 $8.03 +1.75; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1107 yday $1.69 → 09:30 $1.78 +99.63; CRK×86 yday $15.54 → 09:30 $15.45 -7.74; MMED×59 yday $23.76 → 09:30 $23.88 +7.08; SLN×91 yday $14.79 → 09:30 $14.85 +5.46 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.25 | ▲ 09:30 equity $10,321.25 vs yday $10,286.85 (+34.40) | 09:30 open · cash $56.25 (unchanged overnight, no fees) · equity $10,321.25 vs prior close $10,286.85 (+34.40) because holdings re-marked: BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; INO×2469 yday $0.90 → 09:30 $0.93 +74.07; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85 | — |
| 2026-08-14 09:30 ET | **SELL** | `BTSG` | 33 | $59.65 | $2.11 | $-9.15 | $2,022.58 | ▼ -9.15 after sell → book $10,319.13; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 43 | $44.09 | $2.14 | $-85.53 | $3,916.31 | ▼ -85.53 after sell → book $10,316.99; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 39 | $55.29 | $2.13 | $+177.76 | $6,070.49 | ▲ +177.76 after sell → book $10,314.86; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `INO` | 2469 | $0.93 | $30.80 | $+238.08 | $8,335.86 | ▲ +238.08 after sell → book $10,284.06; vs 09:30 mark -30.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 85 | $22.92 | $2.27 | $-39.37 | $10,281.78 | ▼ -39.37 after sell → book $10,281.78; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $9,104.57 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+3.6; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $8,109.84 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 22 | $57.61 | $2.06 | — | $6,840.37 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+5.7; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1371 | $0.94 | $16.96 | — | $5,538.78 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 856 | $1.50 | $11.04 | — | $4,243.74 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 86 | $14.80 | $2.25 | — | $2,968.69 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 298 | $4.31 | $3.84 | — | $1,680.46 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 307 | $4.18 | $3.96 | — | $393.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1285.22 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $393.24 | ▲ 09:30 equity $10,166.90 vs yday $10,119.14 (+47.76) | 09:30 open · cash $393.24 (unchanged overnight, no fees) · equity $10,166.90 vs prior close $10,119.14 (+47.76) because holdings re-marked: VST×8 yday $148.13 → 09:30 $149.37 +9.92; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×22 yday $56.09 → 09:30 $55.37 -15.84; LDI×1371 yday $0.90 → 09:30 $0.91 +13.71; BTBT×856 yday $1.57 → 09:30 $1.52 -42.80; BETR×86 yday $13.73 → 09:30 $13.67 -5.16; ANGX×298 yday $4.37 → 09:30 $4.60 +68.54; HYLN×307 yday $4.06 → 09:30 $4.10 +12.28 | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $1,586.17 | ▲ +15.71 after sell → book $10,164.87; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $2,594.97 | ▲ +14.07 after sell → book $10,162.85; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 22 | $55.37 | $2.08 | $-53.41 | $3,811.04 | ▼ -53.41 after sell → book $10,160.77; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1371 | $0.91 | $16.79 | $-74.87 | $5,037.75 | ▼ -74.87 after sell → book $10,143.99; vs 09:30 mark -16.78 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 856 | $1.52 | $11.19 | $-5.12 | $6,327.67 | ▼ -5.12 after sell → book $10,132.79; vs 09:30 mark -11.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 86 | $13.67 | $2.27 | $-101.70 | $7,501.02 | ▼ -101.70 after sell → book $10,130.52; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 298 | $4.60 | $3.90 | $+78.67 | $8,867.91 | ▲ +78.67 after sell → book $10,126.61; vs 09:30 mark -3.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 307 | $4.10 | $4.02 | $-32.54 | $10,122.59 | ▼ -32.54 after sell → book $10,122.59; vs 09:30 mark -4.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,873.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+6.7; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,729.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+5.8; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,511.28 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+8.3; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 249 | $5.07 | $3.21 | — | $5,245.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=-4.7; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 31 | $39.85 | $2.08 | — | $4,008.21 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1265.32 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 138 | $9.12 | $2.40 | — | $2,747.24 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1265.32 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 40 | $31.30 | $2.11 | — | $1,493.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1265.32 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $282.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-0.8; leftover $1265.32 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $282.23 | ▲ 09:30 equity $10,164.62 vs yday $10,112.86 (+51.76) | 09:30 open · cash $282.23 (unchanged overnight, no fees) · equity $10,164.62 vs prior close $10,112.86 (+51.76) because holdings re-marked: DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; NB×249 yday $4.81 → 09:30 $4.66 -37.35; CDNL×31 yday $39.23 → 09:30 $41.57 +72.54; ABX×138 yday $9.12 → 09:30 $9.03 -12.42; VERA×40 yday $31.63 → 09:30 $31.31 -12.80; CELC×13 yday $92.44 → 09:30 $92.38 -0.78 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,576.14 | ▲ +44.98 after sell → book $10,162.53; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,758.43 | ▲ +38.11 after sell → book $10,160.50; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $4,009.98 | ▲ +33.34 after sell → book $10,158.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NB` | 249 | $4.66 | $3.26 | $-108.57 | $5,167.06 | ▼ -108.57 after sell → book $10,155.21; vs 09:30 mark -3.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 31 | $41.57 | $2.10 | $+49.13 | $6,453.62 | ▲ +49.13 after sell → book $10,153.10; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 138 | $9.03 | $2.44 | $-17.26 | $7,697.33 | ▼ -17.26 after sell → book $10,150.67; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 40 | $31.31 | $2.13 | $-3.84 | $8,947.60 | ▼ -3.84 after sell → book $10,148.54; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $10,146.49 | ▼ -12.01 after sell → book $10,146.49; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.49 | ▲ 09:30 equity $10,146.49 vs yday $10,146.49 (-0.00) | 09:30 open · cash $10,146.49 · no holdings · equity $10,146.49 vs prior close $10,146.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,146.49 | ▲ 09:30 equity $10,146.49 vs yday $10,146.49 (-0.00) | 09:30 open · cash $10,146.49 · no holdings · equity $10,146.49 vs prior close $10,146.49 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 61 | $20.55 | $2.17 | — | $8,890.76 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 61 | $20.65 | $2.17 | — | $7,628.94 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 219 | $5.77 | $2.83 | — | $6,362.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 64 | $19.63 | $2.18 | — | $5,103.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 42 | $29.63 | $2.12 | — | $3,857.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 724 | $1.75 | $9.34 | — | $2,581.07 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,422.73 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 257 | $4.92 | $3.32 | — | $154.98 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1268.31 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $154.98 | ▲ 09:30 equity $10,635.34 vs yday $10,282.06 (+353.28) | 09:30 open · cash $154.98 (unchanged overnight, no fees) · equity $10,635.34 vs prior close $10,282.06 (+353.28) because holdings re-marked: AG×61 yday $21.19 → 09:30 $21.90 +43.31; CDE×61 yday $21.11 → 09:30 $21.75 +39.04; HDSN×219 yday $5.57 → 09:30 $5.67 +21.90; IAG×64 yday $20.50 → 09:30 $21.17 +42.88; KGC×42 yday $31.43 → 09:30 $32.17 +31.08; NFGC×724 yday $1.75 → 09:30 $1.79 +28.96; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×257 yday $4.77 → 09:30 $5.20 +110.51 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 61 | $21.90 | $2.19 | $+77.98 | $1,488.68 | ▲ +77.98 after sell → book $10,633.14; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 61 | $21.75 | $2.19 | $+62.73 | $2,813.24 | ▲ +62.73 after sell → book $10,630.95; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 219 | $5.67 | $2.87 | $-27.60 | $4,052.10 | ▼ -27.60 after sell → book $10,628.08; vs 09:30 mark -2.87 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 64 | $21.17 | $2.20 | $+94.17 | $5,404.78 | ▲ +94.17 after sell → book $10,625.88; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 42 | $32.17 | $2.14 | $+102.43 | $6,753.78 | ▲ +102.43 after sell → book $10,623.74; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 724 | $1.79 | $9.47 | $+10.15 | $8,040.27 | ▲ +10.15 after sell → book $10,614.27; vs 09:30 mark -9.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $9,275.84 | ▲ +77.23 after sell → book $10,612.24; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 257 | $5.20 | $3.37 | $+65.28 | $10,608.87 | ▲ +65.28 after sell → book $10,608.87; vs 09:30 mark -3.37 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 11 | $119.43 | $2.02 | — | $9,293.11 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 77 | $17.20 | $2.22 | — | $7,966.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 6 | $216.30 | $2.01 | — | $6,666.68 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 119 | $11.13 | $2.35 | — | $5,339.87 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 1004 | $1.32 | $12.95 | — | $4,001.64 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 798 | $1.66 | $10.29 | — | $2,666.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $1,418.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1326.11 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 88 | $14.96 | $2.25 | — | $99.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-1.6; leftover $1326.11 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $99.41 | ▲ 09:30 equity $11,259.17 vs yday $10,850.47 (+408.70) | 09:30 open · cash $99.41 (unchanged overnight, no fees) · equity $11,259.17 vs prior close $10,850.47 (+408.70) because holdings re-marked: AU×11 yday $121.22 → 09:30 $120.50 -7.92; AUPH×77 yday $16.65 → 09:30 $16.60 -3.85; AEM×6 yday $216.06 → 09:30 $217.03 +5.82; ARCT×119 yday $13.45 → 09:30 $13.26 -22.61; CYPH×1004 yday $1.42 → 09:30 $1.83 +411.64; BTBT×798 yday $1.53 → 09:30 $1.55 +15.96; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×88 yday $14.74 → 09:30 $14.71 -2.64 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 11 | $120.50 | $2.04 | $+7.70 | $1,422.87 | ▲ +7.70 after sell → book $11,257.13; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 77 | $16.60 | $2.24 | $-50.66 | $2,698.82 | ▼ -50.66 after sell → book $11,254.88; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 6 | $217.03 | $2.03 | $+0.34 | $3,998.98 | ▲ +0.34 after sell → book $11,252.86; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 119 | $13.26 | $2.38 | $+248.74 | $5,574.54 | ▲ +248.74 after sell → book $11,250.48; vs 09:30 mark -2.38 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 1004 | $1.83 | $13.13 | $+485.96 | $7,398.72 | ▲ +485.96 after sell → book $11,237.34; vs 09:30 mark -13.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 798 | $1.55 | $10.44 | $-108.51 | $8,625.19 | ▼ -108.51 after sell → book $11,226.91; vs 09:30 mark -10.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $9,930.41 | ▲ +56.71 after sell → book $11,224.89; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 88 | $14.71 | $2.28 | $-26.53 | $11,222.61 | ▼ -26.53 after sell → book $11,222.61; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,222.61 | ▲ 09:30 equity $11,222.61 vs yday $11,222.61 (+0.00) | 09:30 open · cash $11,222.61 · no holdings · equity $11,222.61 vs prior close $11,222.61 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 58 | $24.00 | $2.16 | — | $9,828.45 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.0; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $8,474.05 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=+9.2; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 268 | $5.23 | $3.46 | — | $7,068.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+10.7; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $5,779.23 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+6.1; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 701 | $2.00 | $9.04 | — | $4,368.19 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1402.83 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 268 | $5.22 | $3.46 | — | $2,965.77 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1402.83 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 94 | $14.86 | $2.27 | — | $1,566.66 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1402.83 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 219 | $6.38 | $2.83 | — | $166.62 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1402.83 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.62 | ▲ 09:30 equity $11,243.98 vs yday $11,243.98 (-0.00) | 09:30 open · cash $166.62 (unchanged overnight, no fees) · equity $11,243.98 vs prior close $11,243.98 (-0.00) because holdings re-marked: MOS×58 yday $23.75 → 09:30 $23.75 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; RZLT×268 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; NPWR×701 yday $2.02 → 09:30 $2.02 +0.00; ALVO×268 yday $5.25 → 09:30 $5.25 +0.00; ALIT×94 yday $14.87 → 09:30 $14.87 +0.00; ZURA×219 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.62 | ▼ 09:30 equity $10,980.00 vs yday $11,195.34 (-215.34) | 09:30 open · cash $166.62 (unchanged overnight, no fees) · equity $10,980.00 vs prior close $11,195.34 (-215.34) because holdings re-marked: MOS×58 yday $23.75 → 09:30 $24.84 +63.22; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; RZLT×268 yday $5.29 → 09:30 $5.01 -75.04; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; NPWR×701 yday $2.02 → 09:30 $1.93 -63.09; ALVO×268 yday $5.25 → 09:30 $4.98 -72.36; ALIT×94 yday $14.87 → 09:30 $14.85 -1.88; ZURA×219 yday $6.50 → 09:30 $6.13 -81.03 | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 22 | $60.07 | $2.08 | $-34.93 | $1,486.08 | ▼ -34.93 after sell → book $10,977.92; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 268 | $5.01 | $3.51 | $-65.93 | $2,825.25 | ▼ -65.93 after sell → book $10,974.41; vs 09:30 mark -3.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $4,105.73 | ▼ -9.24 after sell → book $10,972.39; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 701 | $1.93 | $9.17 | $-67.28 | $5,449.49 | ▼ -67.28 after sell → book $10,963.22; vs 09:30 mark -9.17 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 268 | $4.98 | $3.51 | $-71.29 | $6,780.62 | ▼ -71.29 after sell → book $10,959.71; vs 09:30 mark -3.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 94 | $14.85 | $2.30 | $-5.51 | $8,174.22 | ▼ -5.51 after sell → book $10,957.41; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 219 | $6.13 | $2.87 | $-60.45 | $9,513.82 | ▼ -60.45 after sell → book $10,954.54; vs 09:30 mark -2.87 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 33 | $40.72 | $2.09 | — | $8,167.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+1.8; leftover $1359.12 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 96 | $14.09 | $2.28 | — | $6,813.05 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+1.1; leftover $1359.12 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 524 | $2.59 | $6.76 | — | $5,449.13 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; ret5=+4.2; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 7 | $190.90 | $2.01 | — | $4,110.82 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=-5.1; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 87 | $15.60 | $2.25 | — | $2,751.37 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+7.1; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 47 | $28.89 | $2.13 | — | $1,391.41 | — | union ∩ last_green, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+1.6; leftover $1359.12 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,391.41 | ▲ 09:30 equity $11,074.67 vs yday $11,068.63 (+6.04) | 09:30 open · cash $1,391.41 (unchanged overnight, no fees) · equity $11,074.67 vs prior close $11,068.63 (+6.04) because holdings re-marked: MOS×58 yday $24.16 → 09:30 $24.00 -9.28; RRC×33 yday $41.55 → 09:30 $41.44 -3.63; CRK×96 yday $14.50 → 09:30 $14.42 -7.68; SLI×524 yday $2.61 → 09:30 $2.60 -5.24; ANET×7 yday $202.25 → 09:30 $205.90 +25.55; DLO×87 yday $15.36 → 09:30 $15.33 -2.61; GEN×47 yday $29.64 → 09:30 $29.83 +8.93 | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 7 | $205.90 | $2.03 | $+100.96 | $2,830.67 | ▲ +100.96 after sell → book $11,072.63; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 87 | $15.33 | $2.28 | $-28.02 | $4,162.11 | ▼ -28.02 after sell → book $11,070.36; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 47 | $29.83 | $2.15 | $+39.90 | $5,561.97 | ▲ +39.90 after sell → book $11,068.21; vs 09:30 mark -2.15 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,257.65 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1390.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 82 | $16.95 | $2.24 | — | $2,865.51 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1390.49 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 75 | $18.50 | $2.21 | — | $1,475.80 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1390.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1007 | $1.38 | $12.99 | — | $73.15 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1390.49 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.15 | ▼ 09:30 equity $10,845.73 vs yday $10,965.34 (-119.61) | 09:30 open · cash $73.15 (unchanged overnight, no fees) · equity $10,845.73 vs prior close $10,965.34 (-119.61) because holdings re-marked: MOS×58 yday $23.76 → 09:30 $23.75 -0.58; RRC×33 yday $41.64 → 09:30 $41.11 -17.49; CRK×96 yday $14.62 → 09:30 $14.56 -5.76; SLI×524 yday $2.64 → 09:30 $2.51 -68.12; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×82 yday $16.12 → 09:30 $15.44 -55.76; BZ×75 yday $18.00 → 09:30 $17.89 -8.25; LVWR×1007 yday $1.36 → 09:30 $1.37 +10.07 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 58 | $23.75 | $2.19 | $-18.85 | $1,448.46 | ▼ -18.85 after sell → book $10,843.54; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 33 | $41.11 | $2.11 | $+8.67 | $2,802.98 | ▲ +8.67 after sell → book $10,841.43; vs 09:30 mark -2.11 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 96 | $14.56 | $2.31 | $+40.54 | $4,198.44 | ▲ +40.54 after sell → book $10,839.13; vs 09:30 mark -2.30 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SLI` | 524 | $2.51 | $6.86 | $-55.54 | $5,506.82 | ▼ -55.54 after sell → book $10,832.27; vs 09:30 mark -6.86 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $6,842.81 | ▲ +31.68 after sell → book $10,830.23; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 82 | $15.44 | $2.26 | $-128.32 | $8,106.63 | ▼ -128.32 after sell → book $10,827.97; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 75 | $17.89 | $2.24 | $-50.20 | $9,446.14 | ▼ -50.20 after sell → book $10,825.73; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 1007 | $1.37 | $13.17 | $-36.23 | $10,812.57 | ▼ -36.23 after sell → book $10,812.57; vs 09:30 mark -13.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,812.57 | ▲ 09:30 equity $10,812.57 vs yday $10,812.57 (-0.00) | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,812.57 | ▲ 09:30 equity $10,812.57 vs yday $10,812.57 (-0.00) | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,812.57 | ▲ 09:30 equity $10,812.57 vs yday $10,812.57 (-0.00) | 09:30 open · cash $10,812.57 · no holdings · equity $10,812.57 vs prior close $10,812.57 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 27 | $49.76 | $2.07 | — | $9,466.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,142.97 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 175 | $7.70 | $2.52 | — | $6,792.95 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $5,531.53 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1107 | $1.22 | $14.28 | — | $4,166.71 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1351.57 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 86 | $15.70 | $2.25 | — | $2,814.27 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1351.57 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 59 | $22.78 | $2.17 | — | $1,468.08 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 91 | $14.70 | $2.26 | — | $128.12 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1351.57 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $128.12 | ▲ 09:30 equity $11,718.82 vs yday $11,587.47 (+131.35) | 09:30 open · cash $128.12 (unchanged overnight, no fees) · equity $11,718.82 vs prior close $11,587.47 (+131.35) because holdings re-marked: ATRC×27 yday $52.59 → 09:30 $52.88 +7.83; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; VSTM×175 yday $8.02 → 09:30 $8.03 +1.75; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1107 yday $1.69 → 09:30 $1.78 +99.63; CRK×86 yday $15.54 → 09:30 $15.45 -7.74; MMED×59 yday $23.76 → 09:30 $23.88 +7.08; SLN×91 yday $14.79 → 09:30 $14.85 +5.46 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,499.77 | ▲ +47.65 after sell → book $11,716.71; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 175 | $8.03 | $2.56 | $+52.68 | $2,902.46 | ▲ +52.68 after sell → book $11,714.15; vs 09:30 mark -2.56 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,224.92 | ▲ +61.04 after sell → book $11,712.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 86 | $15.45 | $2.27 | $-26.02 | $5,551.35 | ▼ -26.02 after sell → book $11,709.84; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 59 | $23.88 | $2.19 | $+60.54 | $6,958.08 | ▲ +60.54 after sell → book $11,707.65; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 91 | $14.85 | $2.29 | $+9.10 | $8,307.14 | ▲ +9.10 after sell → book $11,705.36; vs 09:30 mark -2.29 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 45 | $30.65 | $2.12 | — | $6,925.77 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ret5=-2.2; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 132 | $10.41 | $2.39 | — | $5,549.26 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 95 | $14.50 | $2.27 | — | $4,169.49 | — | union ∩ last_green, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 710 | $1.95 | $9.16 | — | $2,775.83 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1384.52 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 387 | $3.57 | $4.99 | — | $1,389.25 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1384.52 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $414.63 | — | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1384.52 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ last_green, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1384.52 |
