# Factor mine action — `union_candle_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ candle, no 🚨

Cash book **+2.13%** ($10,213) · signal-only (no cash/fees) was +1.03%. Starts YES **7/17**. Fills 124 · skips 57 · realized $+542.11.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `candle_capture=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $12.33.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | IREN, TPG, TNDM | — | $79.27 | $10,136.75 | IREN×72, TPG×65, TNDM×142 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $79.27 | IREN×72, TPG×65, TNDM×142 | $10,102.24 | -34.51 | SLG, ANGX, WDC, ADUR, ARX, AIRO, QMLS, TBBB | IREN, TPG, TNDM | $356.14 | $9,852.23 | SLG×21, ANGX×292, WDC×2, ADUR×76, ARX×64, AIRO×113, QMLS×173, TBBB×25 | 09:30 open · cash $79.27 (unchanged overnight, no fees) · equity $10,102.24 vs prior close $10,136.75 (-34.51) because holdings re-marked: IREN×72 yday $44.76 → 09:30 $44.09 -48.24; TPG×65 yday $54.62 → 09:30 $55.29 +43.55; TNDM×142 yday $23.13 → 09:30 $22.92 -29.82 |
| 2026-08-17 | +2.25 | $356.14 | SLG×21, ANGX×292, WDC×2, ADUR×76, ARX×64, AIRO×113, QMLS×173, TBBB×25 | $9,879.81 | +27.58 | DVN, FANG, CDNL, ABX, VERA, HTFL, UMAC, NPWR | SLG, ANGX, WDC, ADUR, ARX, AIRO, QMLS, TBBB | $142.34 | $9,699.31 | DVN×26, FANG×6, CDNL×30, ABX×135, VERA×39, HTFL×29, UMAC×37, NPWR×641 | 09:30 open · cash $356.14 (unchanged overnight, no fees) · equity $9,879.81 vs prior close $9,852.23 (+27.58) because holdings re-marked: SLG×21 yday $56.09 → 09:30 $55.37 -15.12; ANGX×292 yday $4.37 → 09:30 $4.60 +67.16; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ADUR×76 yday $16.17 → 09:30 $15.73 -33.44; ARX×64 yday $19.58 → 09:30 $19.57 -0.64; AIRO×113 yday $9.57 → 09:30 $9.57 +0.00; QMLS×173 yday $7.32 → 09:30 $7.24 -13.84; TBBB×25 yday $47.79 → 09:30 $47.39 -10.00 |
| 2026-08-18 | -6.20 | $142.34 | DVN×26, FANG×6, CDNL×30, ABX×135, VERA×39, HTFL×29, UMAC×37, NPWR×641 | $9,682.19 | -17.12 | — | DVN, FANG, CDNL, ABX, VERA, HTFL, UMAC, NPWR | $9,658.82 | $9,658.82 | — | 09:30 open · cash $142.34 (unchanged overnight, no fees) · equity $9,682.19 vs prior close $9,699.31 (-17.12) because holdings re-marked: DVN×26 yday $47.57 → 09:30 $48.00 +11.18; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×135 yday $9.12 → 09:30 $9.03 -12.15; VERA×39 yday $31.63 → 09:30 $31.31 -12.48; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NPWR×641 yday $1.73 → 09:30 $1.70 -19.23 |
| 2026-08-19 | -7.20 | $9,658.82 | — | $9,658.82 | -0.00 | — | — | $9,658.82 | $9,658.82 | — | 09:30 open · cash $9,658.82 · no holdings · equity $9,658.82 vs prior close $9,658.82 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $9,658.82 | — | $9,658.82 | -0.00 | AG, CDE, IAG, KGC, NFGC, WPM, ABUS, AEM | — | $272.19 | $9,869.89 | AG×58, CDE×58, IAG×61, KGC×40, NFGC×689, WPM×8, ABUS×245, AEM×5 | 09:30 open · cash $9,658.82 · no holdings · equity $9,658.82 vs prior close $9,658.82 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $272.19 | AG×58, CDE×58, IAG×61, KGC×40, NFGC×689, WPM×8, ABUS×245, AEM×5 | $10,208.47 | +338.58 | AU, AUPH, ARCT, CRSP, CYPH, GMAB, BTBT | AG, CDE, IAG, KGC, NFGC, WPM, ABUS | $173.32 | $10,392.06 | AEM×5, AU×10, AUPH×75, ARCT×116, CRSP×21, CYPH×985, GMAB×38, BTBT×783 | 09:30 open · cash $272.19 (unchanged overnight, no fees) · equity $10,208.47 vs prior close $9,869.89 (+338.58) because holdings re-marked: AG×58 yday $21.19 → 09:30 $21.90 +41.18; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×689 yday $1.75 → 09:30 $1.79 +27.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×245 yday $4.77 → 09:30 $5.20 +105.35; AEM×5 yday $212.04 → 09:30 $216.30 +21.30 |
| 2026-08-24 | -5.17 | $173.32 | AEM×5, AU×10, AUPH×75, ARCT×116, CRSP×21, CYPH×985, GMAB×38, BTBT×783 | $10,744.58 | +352.52 | — | AEM, AU, AUPH, ARCT, CRSP, CYPH, GMAB, BTBT | $10,708.59 | $10,708.59 | — | 09:30 open · cash $173.32 (unchanged overnight, no fees) · equity $10,744.58 vs prior close $10,392.06 (+352.52) because holdings re-marked: AEM×5 yday $216.06 → 09:30 $217.03 +4.85; AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×75 yday $16.65 → 09:30 $16.60 -3.75; ARCT×116 yday $13.45 → 09:30 $13.26 -22.04; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×985 yday $1.42 → 09:30 $1.83 +403.85; GMAB×38 yday $33.45 → 09:30 $32.82 -23.94; BTBT×783 yday $1.53 → 09:30 $1.55 +15.66 |
| 2026-08-25 | +1.80 | $10,708.59 | — | $10,708.59 | -0.00 | MOS, INSP, RZLT, HCA, ALVO, ALIT, CYPH, GORO | — | $96.76 | $10,650.55 | MOS×55, INSP×21, RZLT×255, HCA×3, ALVO×256, ALIT×90, CYPH×787, GORO×379 | 09:30 open · cash $10,708.59 · no holdings · equity $10,708.59 vs prior close $10,708.59 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $96.76 | MOS×55, INSP×21, RZLT×255, HCA×3, ALVO×256, ALIT×90, CYPH×787, GORO×379 | $10,650.55 | -0.00 | — | — | $96.76 | $10,678.49 | MOS×55, INSP×21, RZLT×255, HCA×3, ALVO×256, ALIT×90, CYPH×787, GORO×379 | 09:30 open · cash $96.76 (unchanged overnight, no fees) · equity $10,650.55 vs prior close $10,650.55 (-0.00) because holdings re-marked: MOS×55 yday $23.75 → 09:30 $23.75 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; RZLT×255 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALVO×256 yday $5.25 → 09:30 $5.25 +0.00; ALIT×90 yday $14.87 → 09:30 $14.87 +0.00; CYPH×787 yday $1.64 → 09:30 $1.64 +0.00; GORO×379 yday $3.56 → 09:30 $3.56 +0.00 |
| 2026-08-27 | — | $96.76 | MOS×55, INSP×21, RZLT×255, HCA×3, ALVO×256, ALIT×90, CYPH×787, GORO×379 | $10,583.89 | -94.60 | RRC, CRK, DLO, GEN, PLTR | INSP, RZLT, HCA, ALVO, ALIT, CYPH, GORO | $1,725.36 | $10,653.71 | MOS×55, RRC×37, CRK×108, DLO×98, GEN×53, PLTR×8 | 09:30 open · cash $96.76 (unchanged overnight, no fees) · equity $10,583.89 vs prior close $10,678.49 (-94.60) because holdings re-marked: MOS×55 yday $23.75 → 09:30 $24.84 +59.95; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; RZLT×255 yday $5.29 → 09:30 $5.01 -71.40; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALVO×256 yday $5.25 → 09:30 $4.98 -69.12; ALIT×90 yday $14.87 → 09:30 $14.85 -1.80; CYPH×787 yday $1.64 → 09:30 $1.60 -31.48; GORO×379 yday $3.56 → 09:30 $3.77 +79.59 |
| 2026-08-28 | +0.75 | $1,725.36 | MOS×55, RRC×37, CRK×108, DLO×98, GEN×53, PLTR×8 | $10,649.33 | -4.38 | LVWR, GRRR, SIMO, EQ, ZYME | DLO, GEN, PLTR | $151.97 | $10,560.31 | MOS×55, RRC×37, CRK×108, LVWR×903, GRRR×78, SIMO×4, EQ×528, ZYME×42 | 09:30 open · cash $1,725.36 (unchanged overnight, no fees) · equity $10,649.33 vs prior close $10,653.71 (-4.38) because holdings re-marked: MOS×55 yday $24.16 → 09:30 $24.00 -8.80; RRC×37 yday $41.55 → 09:30 $41.44 -4.07; CRK×108 yday $14.50 → 09:30 $14.42 -8.64; DLO×98 yday $15.36 → 09:30 $15.33 -2.94; GEN×53 yday $29.64 → 09:30 $29.83 +10.07; PLTR×8 yday $177.50 → 09:30 $178.75 +10.00 |
| 2026-08-31 | -5.85 | $151.97 | MOS×55, RRC×37, CRK×108, LVWR×903, GRRR×78, SIMO×4, EQ×528, ZYME×42 | $10,331.70 | -228.61 | — | MOS, RRC, CRK, LVWR, GRRR, SIMO, EQ, ZYME | $10,299.94 | $10,299.94 | — | 09:30 open · cash $151.97 (unchanged overnight, no fees) · equity $10,331.70 vs prior close $10,560.31 (-228.61) because holdings re-marked: MOS×55 yday $23.76 → 09:30 $23.75 -0.55; RRC×37 yday $41.64 → 09:30 $41.11 -19.61; CRK×108 yday $14.62 → 09:30 $14.56 -6.48; LVWR×903 yday $1.36 → 09:30 $1.37 +9.03; GRRR×78 yday $15.66 → 09:30 $14.32 -104.52; SIMO×4 yday $255.08 → 09:30 $246.79 -33.16; EQ×528 yday $2.45 → 09:30 $2.37 -42.24; ZYME×42 yday $29.01 → 09:30 $28.27 -31.08 |
| 2026-09-01 | -6.30 | $10,299.94 | — | $10,299.94 | -0.00 | — | — | $10,299.94 | $10,299.94 | — | 09:30 open · cash $10,299.94 · no holdings · equity $10,299.94 vs prior close $10,299.94 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,299.94 | — | $10,299.94 | -0.00 | — | — | $10,299.94 | $10,299.94 | — | 09:30 open · cash $10,299.94 · no holdings · equity $10,299.94 vs prior close $10,299.94 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,299.94 | — | $10,299.94 | -0.00 | ATRC, RVTY, CRK, MMED, ARCT, SID, NVAX, CLYM | — | $62.49 | $10,505.78 | ATRC×25, RVTY×10, CRK×82, MMED×56, ARCT×78, SID×1119, NVAX×125, CLYM×87 | 09:30 open · cash $10,299.94 · no holdings · equity $10,299.94 vs prior close $10,299.94 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $62.49 | ATRC×25, RVTY×10, CRK×82, MMED×56, ARCT×78, SID×1119, NVAX×125, CLYM×87 | $10,658.84 | +153.06 | BVS, HQ, OABI, ALEC, UAMY, FMC | RVTY, CRK, MMED, ARCT, SID, CLYM | $12.33 | $10,213.25 | ATRC×25, NVAX×125, BVS×92, HQ×78, OABI×262, ALEC×494, UAMY×248, FMC×99 | 09:30 open · cash $62.49 (unchanged overnight, no fees) · equity $10,658.84 vs prior close $10,505.78 (+153.06) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; CRK×82 yday $15.54 → 09:30 $15.45 -7.38; MMED×56 yday $23.76 → 09:30 $23.88 +6.72; ARCT×78 yday $16.74 → 09:30 $16.77 +2.34; SID×1119 yday $1.17 → 09:30 $1.36 +212.61; NVAX×125 yday $10.32 → 09:30 $10.41 +11.25; CLYM×87 yday $15.05 → 09:30 $13.96 -94.83 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 72 | $45.98 | $2.21 | — | $6,687.23 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+12.3; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 65 | $50.62 | $2.19 | — | $3,394.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+6.2; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 142 | $23.33 | $2.42 | — | $79.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+19.7; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.27 | ▼ 09:30 equity $10,102.24 vs yday $10,136.75 (-34.51) | 09:30 open · cash $79.27 (unchanged overnight, no fees) · equity $10,102.24 vs prior close $10,136.75 (-34.51) because holdings re-marked: IREN×72 yday $44.76 → 09:30 $44.09 -48.24; TPG×65 yday $54.62 → 09:30 $55.29 +43.55; TNDM×142 yday $23.13 → 09:30 $22.92 -29.82 | — |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 72 | $44.09 | $2.24 | $-140.53 | $3,251.50 | ▼ -140.53 after sell → book $10,099.99; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 65 | $55.29 | $2.22 | $+298.93 | $6,843.13 | ▲ +298.93 after sell → book $10,097.77; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 142 | $22.92 | $2.47 | $-63.10 | $10,095.30 | ▼ -63.10 after sell → book $10,095.30; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $8,883.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+5.7; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 292 | $4.31 | $3.77 | — | $7,621.15 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $6,612.16 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $5,355.94 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $4,101.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 113 | $11.12 | $2.33 | — | $2,842.39 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 173 | $7.29 | $2.51 | — | $1,578.71 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $356.14 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $356.14 | ▲ 09:30 equity $9,879.81 vs yday $9,852.23 (+27.58) | 09:30 open · cash $356.14 (unchanged overnight, no fees) · equity $9,879.81 vs prior close $9,852.23 (+27.58) because holdings re-marked: SLG×21 yday $56.09 → 09:30 $55.37 -15.12; ANGX×292 yday $4.37 → 09:30 $4.60 +67.16; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; ADUR×76 yday $16.17 → 09:30 $15.73 -33.44; ARX×64 yday $19.58 → 09:30 $19.57 -0.64; AIRO×113 yday $9.57 → 09:30 $9.57 +0.00; QMLS×173 yday $7.32 → 09:30 $7.24 -13.84; TBBB×25 yday $47.79 → 09:30 $47.39 -10.00 | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $1,516.84 | ▼ -51.17 after sell → book $9,877.74; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 292 | $4.60 | $3.83 | $+77.09 | $2,856.21 | ▲ +77.09 after sell → book $9,873.91; vs 09:30 mark -3.83 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $3,905.26 | ▲ +40.05 after sell → book $9,871.90; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $5,098.50 | ▼ -62.98 after sell → book $9,869.66; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $6,348.77 | ▼ -4.38 after sell → book $9,867.45; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 113 | $9.57 | $2.36 | $-179.84 | $7,427.83 | ▼ -179.84 after sell → book $9,865.10; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 173 | $7.24 | $2.55 | $-13.71 | $8,677.80 | ▼ -13.71 after sell → book $9,862.55; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $9,860.46 | ▼ -39.90 after sell → book $9,860.46; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $8,657.72 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+6.7; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $7,439.51 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+8.3; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $6,241.93 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 135 | $9.12 | $2.40 | — | $5,008.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $3,785.53 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=-3.8; leftover $1232.56 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $2,587.78 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1232.56 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,381.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 641 | $1.92 | $8.27 | — | $142.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $142.34 | ▼ 09:30 equity $9,682.19 vs yday $9,699.31 (-17.12) | 09:30 open · cash $142.34 (unchanged overnight, no fees) · equity $9,682.19 vs prior close $9,699.31 (-17.12) because holdings re-marked: DVN×26 yday $47.57 → 09:30 $48.00 +11.18; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×135 yday $9.12 → 09:30 $9.03 -12.15; VERA×39 yday $31.63 → 09:30 $31.31 -12.48; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NPWR×641 yday $1.73 → 09:30 $1.70 -19.23 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $1,388.25 | ▲ +43.16 after sell → book $9,680.10; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $2,639.80 | ▲ +33.34 after sell → book $9,678.07; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $3,884.80 | ▲ +47.42 after sell → book $9,675.97; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 135 | $9.03 | $2.43 | $-16.97 | $5,101.43 | ▼ -16.97 after sell → book $9,673.55; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $6,320.39 | ▼ -3.84 after sell → book $9,671.42; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $7,521.79 | ▲ +3.66 after sell → book $9,669.32; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $8,577.50 | ▼ -150.74 after sell → book $9,667.20; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 641 | $1.70 | $8.39 | $-157.67 | $9,658.82 | ▼ -157.67 after sell → book $9,658.82; vs 09:30 mark -8.38 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,658.82 | ▲ 09:30 equity $9,658.82 vs yday $9,658.82 (-0.00) | 09:30 open · cash $9,658.82 · no holdings · equity $9,658.82 vs prior close $9,658.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,658.82 | ▲ 09:30 equity $9,658.82 vs yday $9,658.82 (-0.00) | 09:30 open · cash $9,658.82 · no holdings · equity $9,658.82 vs prior close $9,658.82 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,464.75 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $7,264.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $6,065.28 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $4,877.97 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 689 | $1.75 | $8.89 | — | $3,663.34 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $2,505.00 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 245 | $4.92 | $3.16 | — | $1,296.44 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $272.19 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $272.19 | ▲ 09:30 equity $10,208.47 vs yday $9,869.89 (+338.58) | 09:30 open · cash $272.19 (unchanged overnight, no fees) · equity $10,208.47 vs prior close $9,869.89 (+338.58) because holdings re-marked: AG×58 yday $21.19 → 09:30 $21.90 +41.18; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×689 yday $1.75 → 09:30 $1.79 +27.56; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×245 yday $4.77 → 09:30 $5.20 +105.35; AEM×5 yday $212.04 → 09:30 $216.30 +21.30 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,540.20 | ▲ +73.95 after sell → book $10,206.28; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $2,799.52 | ▲ +59.45 after sell → book $10,204.10; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $4,088.70 | ▲ +89.57 after sell → book $10,201.91; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $5,373.36 | ▲ +97.36 after sell → book $10,199.77; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 689 | $1.79 | $9.01 | $+9.66 | $6,597.66 | ▲ +9.66 after sell → book $10,190.76; vs 09:30 mark -9.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $7,833.23 | ▲ +77.23 after sell → book $10,188.73; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 245 | $5.20 | $3.21 | $+62.23 | $9,104.02 | ▲ +62.23 after sell → book $10,185.52; vs 09:30 mark -3.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,907.70 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $6,615.48 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 116 | $11.13 | $2.34 | — | $5,322.06 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $4,065.89 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 985 | $1.32 | $12.71 | — | $2,752.98 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $1,483.20 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 783 | $1.66 | $10.10 | — | $173.32 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $173.32 | ▲ 09:30 equity $10,744.58 vs yday $10,392.06 (+352.52) | 09:30 open · cash $173.32 (unchanged overnight, no fees) · equity $10,744.58 vs prior close $10,392.06 (+352.52) because holdings re-marked: AEM×5 yday $216.06 → 09:30 $217.03 +4.85; AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×75 yday $16.65 → 09:30 $16.60 -3.75; ARCT×116 yday $13.45 → 09:30 $13.26 -22.04; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×985 yday $1.42 → 09:30 $1.83 +403.85; GMAB×38 yday $33.45 → 09:30 $32.82 -23.94; BTBT×783 yday $1.53 → 09:30 $1.55 +15.66 | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $1,256.44 | ▲ +58.87 after sell → book $10,742.55; vs 09:30 mark -2.03 | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $2,459.40 | ▲ +6.64 after sell → book $10,740.51; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.60 | $2.24 | $-49.45 | $3,702.17 | ▼ -49.45 after sell → book $10,738.28; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 116 | $13.26 | $2.37 | $+242.37 | $5,237.96 | ▲ +242.37 after sell → book $10,735.91; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $6,470.47 | ▼ -23.66 after sell → book $10,733.83; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 985 | $1.83 | $12.88 | $+476.76 | $8,260.14 | ▲ +476.76 after sell → book $10,720.95; vs 09:30 mark -12.88 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $9,505.18 | ▼ -24.75 after sell → book $10,718.83; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 783 | $1.55 | $10.24 | $-106.47 | $10,708.59 | ▼ -106.47 after sell → book $10,708.59; vs 09:30 mark -10.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,708.59 | ▲ 09:30 equity $10,708.59 vs yday $10,708.59 (-0.00) | 09:30 open · cash $10,708.59 · no holdings · equity $10,708.59 vs prior close $10,708.59 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $9,386.43 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+13.0; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $8,093.51 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+9.2; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 255 | $5.23 | $3.29 | — | $6,756.57 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+10.7; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $5,466.85 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.1; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 256 | $5.22 | $3.30 | — | $4,127.23 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1338.57 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 90 | $14.86 | $2.26 | — | $2,787.57 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1338.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 787 | $1.70 | $10.15 | — | $1,439.51 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 379 | $3.53 | $4.89 | — | $96.76 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.76 | ▲ 09:30 equity $10,650.55 vs yday $10,650.55 (-0.00) | 09:30 open · cash $96.76 (unchanged overnight, no fees) · equity $10,650.55 vs prior close $10,650.55 (-0.00) because holdings re-marked: MOS×55 yday $23.75 → 09:30 $23.75 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; RZLT×255 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALVO×256 yday $5.25 → 09:30 $5.25 +0.00; ALIT×90 yday $14.87 → 09:30 $14.87 +0.00; CYPH×787 yday $1.64 → 09:30 $1.64 +0.00; GORO×379 yday $3.56 → 09:30 $3.56 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $96.76 | ▼ 09:30 equity $10,583.89 vs yday $10,678.49 (-94.60) | 09:30 open · cash $96.76 (unchanged overnight, no fees) · equity $10,583.89 vs prior close $10,678.49 (-94.60) because holdings re-marked: MOS×55 yday $23.75 → 09:30 $24.84 +59.95; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; RZLT×255 yday $5.29 → 09:30 $5.01 -71.40; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALVO×256 yday $5.25 → 09:30 $4.98 -69.12; ALIT×90 yday $14.87 → 09:30 $14.85 -1.80; CYPH×787 yday $1.64 → 09:30 $1.60 -31.48; GORO×379 yday $3.56 → 09:30 $3.77 +79.59 | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 21 | $60.07 | $2.07 | $-33.53 | $1,356.15 | ▼ -33.53 after sell → book $10,581.81; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 255 | $5.01 | $3.34 | $-62.73 | $2,630.36 | ▼ -62.73 after sell → book $10,578.47; vs 09:30 mark -3.34 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $3,910.84 | ▼ -9.24 after sell → book $10,576.45; vs 09:30 mark -2.02 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 256 | $4.98 | $3.36 | $-68.10 | $5,182.37 | ▼ -68.10 after sell → book $10,573.10; vs 09:30 mark -3.35 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 90 | $14.85 | $2.29 | $-5.45 | $6,516.58 | ▼ -5.45 after sell → book $10,570.81; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 787 | $1.60 | $10.29 | $-99.15 | $7,765.49 | ▼ -99.15 after sell → book $10,560.52; vs 09:30 mark -10.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 379 | $3.77 | $4.96 | $+81.11 | $9,189.35 | ▲ +81.11 after sell → book $10,555.55; vs 09:30 mark -4.97 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 37 | $40.72 | $2.10 | — | $7,680.61 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.8; leftover $1531.56 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 108 | $14.09 | $2.31 | — | $6,156.58 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.1; leftover $1531.56 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 98 | $15.60 | $2.28 | — | $4,625.49 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+7.1; leftover $1531.56 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 53 | $28.89 | $2.15 | — | $3,092.18 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+1.6; leftover $1531.56 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 8 | $170.60 | $2.01 | — | $1,725.36 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+3.4; leftover $1531.56 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,725.36 | ▼ 09:30 equity $10,649.33 vs yday $10,653.71 (-4.38) | 09:30 open · cash $1,725.36 (unchanged overnight, no fees) · equity $10,649.33 vs prior close $10,653.71 (-4.38) because holdings re-marked: MOS×55 yday $24.16 → 09:30 $24.00 -8.80; RRC×37 yday $41.55 → 09:30 $41.44 -4.07; CRK×108 yday $14.50 → 09:30 $14.42 -8.64; DLO×98 yday $15.36 → 09:30 $15.33 -2.94; GEN×53 yday $29.64 → 09:30 $29.83 +10.07; PLTR×8 yday $177.50 → 09:30 $178.75 +10.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 98 | $15.33 | $2.31 | $-31.06 | $3,225.39 | ▼ -31.06 after sell → book $10,647.02; vs 09:30 mark -2.31 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 53 | $29.83 | $2.17 | $+45.50 | $4,804.21 | ▲ +45.50 after sell → book $10,644.85; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 8 | $178.75 | $2.04 | $+61.15 | $6,232.17 | ▲ +61.15 after sell → book $10,642.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 903 | $1.38 | $11.65 | — | $4,974.38 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1246.43 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 78 | $15.94 | $2.22 | — | $3,728.84 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1246.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $272.00 | $2.00 | — | $2,638.84 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ⚪; ret5=-3.9; leftover $1246.43 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 528 | $2.36 | $6.81 | — | $1,385.95 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ret5=-2.1; leftover $1246.43 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 42 | $29.33 | $2.12 | — | $151.97 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1246.43 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.97 | ▼ 09:30 equity $10,331.70 vs yday $10,560.31 (-228.61) | 09:30 open · cash $151.97 (unchanged overnight, no fees) · equity $10,331.70 vs prior close $10,560.31 (-228.61) because holdings re-marked: MOS×55 yday $23.76 → 09:30 $23.75 -0.55; RRC×37 yday $41.64 → 09:30 $41.11 -19.61; CRK×108 yday $14.62 → 09:30 $14.56 -6.48; LVWR×903 yday $1.36 → 09:30 $1.37 +9.03; GRRR×78 yday $15.66 → 09:30 $14.32 -104.52; SIMO×4 yday $255.08 → 09:30 $246.79 -33.16; EQ×528 yday $2.45 → 09:30 $2.37 -42.24; ZYME×42 yday $29.01 → 09:30 $28.27 -31.08 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.75 | $2.18 | $-18.08 | $1,456.05 | ▼ -18.08 after sell → book $10,329.53; vs 09:30 mark -2.17 | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 37 | $41.11 | $2.12 | $+10.21 | $2,974.99 | ▲ +10.21 after sell → book $10,327.40; vs 09:30 mark -2.13 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 108 | $14.56 | $2.34 | $+46.10 | $4,545.13 | ▲ +46.10 after sell → book $10,325.06; vs 09:30 mark -2.34 | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 903 | $1.37 | $11.81 | $-32.49 | $5,770.43 | ▼ -32.49 after sell → book $10,313.25; vs 09:30 mark -11.81 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 78 | $14.32 | $2.25 | $-130.83 | $6,885.14 | ▼ -130.83 after sell → book $10,311.00; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 4 | $246.79 | $2.02 | $-104.86 | $7,870.28 | ▼ -104.86 after sell → book $10,308.98; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EQ` | 528 | $2.37 | $6.91 | $-8.44 | $9,114.73 | ▼ -8.44 after sell → book $10,302.07; vs 09:30 mark -6.91 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 42 | $28.27 | $2.14 | $-48.77 | $10,299.94 | ▼ -48.77 after sell → book $10,299.94; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,299.94 | ▲ 09:30 equity $10,299.94 vs yday $10,299.94 (-0.00) | 09:30 open · cash $10,299.94 · no holdings · equity $10,299.94 vs prior close $10,299.94 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,299.94 | ▲ 09:30 equity $10,299.94 vs yday $10,299.94 (-0.00) | 09:30 open · cash $10,299.94 · no holdings · equity $10,299.94 vs prior close $10,299.94 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,299.94 | ▲ 09:30 equity $10,299.94 vs yday $10,299.94 (-0.00) | 09:30 open · cash $10,299.94 · no holdings · equity $10,299.94 vs prior close $10,299.94 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $49.76 | $2.06 | — | $9,053.87 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $7,792.45 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 82 | $15.70 | $2.24 | — | $6,502.81 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1287.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 56 | $22.78 | $2.16 | — | $5,224.98 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.46 | $2.22 | — | $3,938.87 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 1119 | $1.15 | $14.44 | — | $2,637.59 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1287.49 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 125 | $10.27 | $2.37 | — | $1,351.47 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 87 | $14.79 | $2.25 | — | $62.49 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+5.8; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.49 | ▲ 09:30 equity $10,658.84 vs yday $10,505.78 (+153.06) | 09:30 open · cash $62.49 (unchanged overnight, no fees) · equity $10,658.84 vs prior close $10,505.78 (+153.06) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; CRK×82 yday $15.54 → 09:30 $15.45 -7.38; MMED×56 yday $23.76 → 09:30 $23.88 +6.72; ARCT×78 yday $16.74 → 09:30 $16.77 +2.34; SID×1119 yday $1.17 → 09:30 $1.36 +212.61; NVAX×125 yday $10.32 → 09:30 $10.41 +11.25; CLYM×87 yday $15.05 → 09:30 $13.96 -94.83 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $1,384.95 | ▲ +61.04 after sell → book $10,656.80; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 82 | $15.45 | $2.26 | $-25.00 | $2,649.59 | ▼ -25.00 after sell → book $10,654.54; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 56 | $23.88 | $2.18 | $+57.26 | $3,984.69 | ▲ +57.26 after sell → book $10,652.36; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 78 | $16.77 | $2.25 | $+19.71 | $5,290.50 | ▲ +19.71 after sell → book $10,650.11; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 1119 | $1.36 | $14.63 | $+205.92 | $6,797.71 | ▲ +205.92 after sell → book $10,635.48; vs 09:30 mark -14.63 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 87 | $13.96 | $2.28 | $-76.74 | $8,009.96 | ▼ -76.74 after sell → book $10,633.21; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 92 | $14.50 | $2.27 | — | $6,673.69 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1334.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 78 | $17.06 | $2.22 | — | $5,340.79 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+17.3; leftover $1334.99 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 262 | $5.08 | $3.38 | — | $4,006.45 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1334.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 494 | $2.70 | $6.37 | — | $2,666.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1334.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 248 | $5.37 | $3.20 | — | $1,331.31 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=-0.4; leftover $1334.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 99 | $13.30 | $2.29 | — | $12.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $1334.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1531.56 < 1 share @ 1746.33 |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ELMT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `TRLV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OMER` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FOX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KBR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SIBN` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HELP` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 25 | 2026-09-03 @ $49.76 | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1287.49 |
| `NVAX` | 125 | 2026-09-03 @ $10.27 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1287.49 |
| `BVS` | 92 | 2026-09-04 @ $14.50 | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1334.99 |
| `HQ` | 78 | 2026-09-04 @ $17.06 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+17.3; leftover $1334.99 |
| `OABI` | 262 | 2026-09-04 @ $5.08 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1334.99 |
| `ALEC` | 494 | 2026-09-04 @ $2.70 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1334.99 |
| `UAMY` | 248 | 2026-09-04 @ $5.37 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=-0.4; leftover $1334.99 |
| `FMC` | 99 | 2026-09-04 @ $13.30 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $1334.99 |
