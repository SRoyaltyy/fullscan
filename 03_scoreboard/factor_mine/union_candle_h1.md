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

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | IREN, TPG, TNDM | — | $79.27 | $10,057.48 | $10,136.75 | IREN×72, TPG×65, TNDM×142 | BUY IREN x72 @ 45.98; BUY TPG x65 @ 50.62; BUY TNDM x142 @ 23.33 |
| 2026-08-14 | +5.50 | $79.27 | IREN×72, TPG×65, TNDM×142 | SLG, ANGX, WDC, ADUR, ARX, AIRO, QMLS, TBBB | IREN, TPG, TNDM | $356.14 | $9,496.09 | $9,852.23 | SLG×21, ANGX×292, WDC×2, ADUR×76, ARX×64, AIRO×113, QMLS×173, TBBB×25 | SELL IREN (dropped from list after 1 sess (min 1)); SELL TPG (dropped from list after 1 sess (min 1)); SELL TNDM (dropped from list after 1 sess (min 1)); BUY SLG x21 @ 57.61; BUY ANGX x292 @ 4.31; BUY WDC x2 @ 503.50; BUY ADUR x76 @ 16.50; BUY ARX x64 @ 19.57; BUY AIRO x113 @ 11.12; BUY QMLS x173 @ 7.29; BUY TBBB x25 @ 48.82 |
| 2026-08-17 | +2.25 | $356.14 | SLG×21, ANGX×292, WDC×2, ADUR×76, ARX×64, AIRO×113, QMLS×173, TBBB×25 | DVN, FANG, CDNL, ABX, VERA, HTFL, UMAC, NPWR | SLG, ANGX, WDC, ADUR, ARX, AIRO, QMLS, TBBB | $142.34 | $9,556.97 | $9,699.31 | DVN×26, FANG×6, CDNL×30, ABX×135, VERA×39, HTFL×29, UMAC×37, NPWR×641 | SELL SLG (dropped from list after 1 sess (min 1)); SELL ANGX (dropped from list after 1 sess (min 1)); SELL WDC (dropped from list after 1 sess (min 1)); SELL ADUR (dropped from list after 1 sess (min 1)); SELL ARX (dropped from list after 1 sess (min 1)); SELL AIRO (dropped from list after 1 sess (min 1)); SELL QMLS (dropped from list after 1 sess (min 1)); SELL TBBB (dropped from list after 1 sess (min 1)); BUY DVN x26 @ 46.18; BUY FANG x6 @ 202.70; BUY CDNL x30 @ 39.85; BUY ABX x135 @ 9.12; BUY VERA x39 @ 31.30; BUY HTFL x29 @ 41.23; BUY UMAC x37 @ 32.55; BUY NPWR x641 @ 1.92 |
| 2026-08-18 | -6.20 | $142.34 | DVN×26, FANG×6, CDNL×30, ABX×135, VERA×39, HTFL×29, UMAC×37, NPWR×641 | — | DVN, FANG, CDNL, ABX, VERA, HTFL, UMAC, NPWR | $9,658.82 | $0.00 | $9,658.82 | — | SELL DVN (dropped from list after 1 sess (min 1)); SELL FANG (dropped from list after 1 sess (min 1)); SELL CDNL (dropped from list after 1 sess (min 1)); SELL ABX (dropped from list after 1 sess (min 1)); SELL VERA (dropped from list after 1 sess (min 1)); SELL HTFL (dropped from list after 1 sess (min 1)); SELL UMAC (dropped from list after 1 sess (min 1)); SELL NPWR (dropped from list after 1 sess (min 1)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,658.82 | — | — | — | $9,658.82 | $0.00 | $9,658.82 | — | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,658.82 | — | AG, CDE, IAG, KGC, NFGC, WPM, ABUS, AEM | — | $272.19 | $9,597.70 | $9,869.89 | AG×58, CDE×58, IAG×61, KGC×40, NFGC×689, WPM×8, ABUS×245, AEM×5 | BUY AG x58 @ 20.55; BUY CDE x58 @ 20.65; BUY IAG x61 @ 19.63; BUY KGC x40 @ 29.63; BUY NFGC x689 @ 1.75; BUY WPM x8 @ 144.54; BUY ABUS x245 @ 4.92; BUY AEM x5 @ 204.45 |
| 2026-08-21 | +3.25 | $272.19 | AG×58, CDE×58, IAG×61, KGC×40, NFGC×689, WPM×8, ABUS×245, AEM×5 | AU, AUPH, ARCT, CRSP, CYPH, GMAB, BTBT | AG, CDE, IAG, KGC, NFGC, WPM, ABUS | $173.32 | $10,218.74 | $10,392.06 | AEM×5, AU×10, AUPH×75, ARCT×116, CRSP×21, CYPH×985, GMAB×38, BTBT×783 | SELL AG (dropped from list after 1 sess (min 1)); SELL CDE (dropped from list after 1 sess (min 1)); SELL IAG (dropped from list after 1 sess (min 1)); SELL KGC (dropped from list after 1 sess (min 1)); SELL NFGC (dropped from list after 1 sess (min 1)); SELL WPM (dropped from list after 1 sess (min 1)); SELL ABUS (dropped from list after 1 sess (min 1)); BUY AU x10 @ 119.43; BUY AUPH x75 @ 17.20; BUY ARCT x116 @ 11.13; BUY CRSP x21 @ 59.72; BUY CYPH x985 @ 1.32; BUY GMAB x38 @ 33.36; BUY BTBT x783 @ 1.66 |
| 2026-08-24 | -5.17 | $173.32 | AEM×5, AU×10, AUPH×75, ARCT×116, CRSP×21, CYPH×985, GMAB×38, BTBT×783 | — | AEM, AU, AUPH, ARCT, CRSP, CYPH, GMAB, BTBT | $10,708.59 | $0.00 | $10,708.59 | — | SELL AEM (dropped from list after 2 sess (min 1)); SELL AU (dropped from list after 1 sess (min 1)); SELL AUPH (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL CRSP (dropped from list after 1 sess (min 1)); SELL CYPH (dropped from list after 1 sess (min 1)); SELL GMAB (dropped from list after 1 sess (min 1)); SELL BTBT (dropped from list after 1 sess (min 1)); hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $10,708.59 | — | MOS, INSP, RZLT, HCA, ALVO, ALIT, CYPH, GORO | — | $96.76 | $10,553.79 | $10,650.55 | MOS×55, INSP×21, RZLT×255, HCA×3, ALVO×256, ALIT×90, CYPH×787, GORO×379 | BUY MOS x55 @ 24.00; BUY INSP x21 @ 61.47; BUY RZLT x255 @ 5.23; BUY HCA x3 @ 429.24; BUY ALVO x256 @ 5.22; BUY ALIT x90 @ 14.86; BUY CYPH x787 @ 1.70; BUY GORO x379 @ 3.53 |
| 2026-08-26 | +2.02 | $96.76 | MOS×55, INSP×21, RZLT×255, HCA×3, ALVO×256, ALIT×90, CYPH×787, GORO×379 | — | — | $96.76 | $10,581.73 | $10,678.49 | MOS×55, INSP×21, RZLT×255, HCA×3, ALVO×256, ALIT×90, CYPH×787, GORO×379 | hold MOS,INSP,RZLT,HCA,ALVO,ALIT,CYPH,GORO |
| 2026-08-27 | — | $96.76 | MOS×55, INSP×21, RZLT×255, HCA×3, ALVO×256, ALIT×90, CYPH×787, GORO×379 | RRC, CRK, DLO, GEN, PLTR | INSP, RZLT, HCA, ALVO, ALIT, CYPH, GORO | $1,725.36 | $8,928.35 | $10,653.71 | MOS×55, RRC×37, CRK×108, DLO×98, GEN×53, PLTR×8 | SELL INSP (dropped from list after 2 sess (min 1)); SELL RZLT (dropped from list after 2 sess (min 1)); SELL HCA (dropped from list after 2 sess (min 1)); SELL ALVO (dropped from list after 2 sess (min 1)); SELL ALIT (dropped from list after 2 sess (min 1)); SELL CYPH (dropped from list after 2 sess (min 1)); SELL GORO (dropped from list after 2 sess (min 1)); BUY RRC x37 @ 40.72; BUY CRK x108 @ 14.09; BUY DLO x98 @ 15.60; BUY GEN x53 @ 28.89; BUY PLTR x8 @ 170.60 |
| 2026-08-28 | +0.75 | $1,725.36 | MOS×55, RRC×37, CRK×108, DLO×98, GEN×53, PLTR×8 | LVWR, GRRR, SIMO, EQ, ZYME | DLO, GEN, PLTR | $151.97 | $10,408.34 | $10,560.31 | MOS×55, RRC×37, CRK×108, LVWR×903, GRRR×78, SIMO×4, EQ×528, ZYME×42 | SELL DLO (dropped from list after 1 sess (min 1)); SELL GEN (dropped from list after 1 sess (min 1)); SELL PLTR (dropped from list after 1 sess (min 1)); BUY LVWR x903 @ 1.38; BUY GRRR x78 @ 15.94; BUY SIMO x4 @ 272.00; BUY EQ x528 @ 2.36; BUY ZYME x42 @ 29.33 |
| 2026-08-31 | -5.85 | $151.97 | MOS×55, RRC×37, CRK×108, LVWR×903, GRRR×78, SIMO×4, EQ×528, ZYME×42 | — | MOS, RRC, CRK, LVWR, GRRR, SIMO, EQ, ZYME | $10,299.94 | $0.00 | $10,299.94 | — | SELL MOS (dropped from list after 4 sess (min 1)); SELL RRC (dropped from list after 2 sess (min 1)); SELL CRK (dropped from list after 2 sess (min 1)); SELL LVWR (dropped from list after 1 sess (min 1)); SELL GRRR (dropped from list after 1 sess (min 1)); SELL SIMO (dropped from list after 1 sess (min 1)); SELL EQ (dropped from list after 1 sess (min 1)); SELL ZYME (dropped from list after 1 sess (min 1)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $10,299.94 | — | — | — | $10,299.94 | $0.00 | $10,299.94 | — | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $10,299.94 | — | — | — | $10,299.94 | $0.00 | $10,299.94 | — | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $10,299.94 | — | ATRC, RVTY, CRK, MMED, ARCT, SID, NVAX, CLYM | — | $62.49 | $10,443.29 | $10,505.78 | ATRC×25, RVTY×10, CRK×82, MMED×56, ARCT×78, SID×1119, NVAX×125, CLYM×87 | BUY ATRC x25 @ 49.76; BUY RVTY x10 @ 125.94; BUY CRK x82 @ 15.70; BUY MMED x56 @ 22.78; BUY ARCT x78 @ 16.46; BUY SID x1119 @ 1.15; BUY NVAX x125 @ 10.27; BUY CLYM x87 @ 14.79 |
| 2026-09-04 | — | $62.49 | ATRC×25, RVTY×10, CRK×82, MMED×56, ARCT×78, SID×1119, NVAX×125, CLYM×87 | BVS, HQ, OABI, ALEC, UAMY, FMC | RVTY, CRK, MMED, ARCT, SID, CLYM | $12.33 | $10,200.92 | $10,213.25 | ATRC×25, NVAX×125, BVS×92, HQ×78, OABI×262, ALEC×494, UAMY×248, FMC×99 | SELL RVTY (dropped from list after 1 sess (min 1)); SELL CRK (dropped from list after 1 sess (min 1)); SELL MMED (dropped from list after 1 sess (min 1)); SELL ARCT (dropped from list after 1 sess (min 1)); SELL SID (dropped from list after 1 sess (min 1)); SELL CLYM (dropped from list after 1 sess (min 1)); BUY BVS x92 @ 14.50; BUY HQ x78 @ 17.06; BUY OABI x262 @ 5.08; BUY ALEC x494 @ 2.70; BUY UAMY x248 @ 5.37; BUY FMC x99 @ 13.30 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 72 | $45.98 | $2.21 | — | $6,687.23 | ▼ $9,997.79 (-2.21) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+12.3; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 65 | $50.62 | $2.19 | — | $3,394.54 | ▼ $9,995.61 (-4.39) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+6.2; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 142 | $23.33 | $2.42 | — | $79.27 | ▼ $9,993.19 (-6.81) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+19.7; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **SELL** | `IREN` | 72 | $44.09 | $2.24 | $-140.53 | $3,251.50 | ▲ $10,099.99 (+99.99) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **SELL** | `TPG` | 65 | $55.29 | $2.22 | $+298.93 | $6,843.13 | ▲ $10,097.77 (+97.77) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **SELL** | `TNDM` | 142 | $22.92 | $2.47 | $-63.10 | $10,095.30 | ▲ $10,095.30 (+95.30) | dropped from list after 1 sess (min 1) | — |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $8,883.44 | ▲ $10,093.25 (+93.25) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+5.7; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 292 | $4.31 | $3.77 | — | $7,621.15 | ▲ $10,089.48 (+89.48) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $6,612.16 | ▲ $10,087.49 (+87.49) | union ∩ candle, no 🚨; gate candle_capture=True; list probable; 🔵; ⚪; ret5=+7.9; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 76 | $16.50 | $2.22 | — | $5,355.94 | ▲ $10,085.27 (+85.27) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 64 | $19.57 | $2.18 | — | $4,101.28 | ▲ $10,083.09 (+83.09) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 113 | $11.12 | $2.33 | — | $2,842.39 | ▲ $10,080.76 (+80.76) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 173 | $7.29 | $2.51 | — | $1,578.71 | ▲ $10,078.25 (+78.25) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `TBBB` | 25 | $48.82 | $2.06 | — | $356.14 | ▲ $10,076.18 (+76.18) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $1261.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $1,516.84 | ▼ $9,877.74 (-122.26) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 292 | $4.60 | $3.83 | $+77.09 | $2,856.21 | ▼ $9,873.91 (-126.09) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WDC` | 2 | $525.53 | $2.02 | $+40.05 | $3,905.26 | ▼ $9,871.90 (-128.10) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 76 | $15.73 | $2.24 | $-62.98 | $5,098.50 | ▼ $9,869.66 (-130.34) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 64 | $19.57 | $2.20 | $-4.38 | $6,348.77 | ▼ $9,867.45 (-132.55) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 113 | $9.57 | $2.36 | $-179.84 | $7,427.83 | ▼ $9,865.10 (-134.90) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 173 | $7.24 | $2.55 | $-13.71 | $8,677.80 | ▼ $9,862.55 (-137.45) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `TBBB` | 25 | $47.39 | $2.08 | $-39.90 | $9,860.46 | ▼ $9,860.46 (-139.54) | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 26 | $46.18 | $2.07 | — | $8,657.72 | ▼ $9,858.40 (-141.60) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+6.7; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $7,439.51 | ▼ $9,856.39 (-143.61) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+8.3; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $6,241.93 | ▼ $9,854.31 (-145.69) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 135 | $9.12 | $2.40 | — | $5,008.33 | ▼ $9,851.91 (-148.09) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1232.56 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $3,785.53 | ▼ $9,849.81 (-150.19) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; ret5=-3.8; leftover $1232.56 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $2,587.78 | ▼ $9,847.73 (-152.27) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1232.56 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $1,381.33 | ▼ $9,845.63 (-154.37) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 641 | $1.92 | $8.27 | — | $142.34 | ▼ $9,837.36 (-162.64) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1232.56 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 26 | $48.00 | $2.09 | $+43.16 | $1,388.25 | ▼ $9,680.10 (-319.90) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $2,639.80 | ▼ $9,678.07 (-321.93) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $3,884.80 | ▼ $9,675.97 (-324.03) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 135 | $9.03 | $2.43 | $-16.97 | $5,101.43 | ▼ $9,673.55 (-326.45) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $6,320.39 | ▼ $9,671.42 (-328.58) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $7,521.79 | ▼ $9,669.32 (-330.68) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $8,577.50 | ▼ $9,667.20 (-332.80) | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 641 | $1.70 | $8.39 | $-157.67 | $9,658.82 | ▼ $9,658.82 (-341.18) | dropped from list after 1 sess (min 1) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,464.75 | ▼ $9,656.65 (-343.35) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $7,264.89 | ▼ $9,654.49 (-345.51) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $6,065.28 | ▼ $9,652.31 (-347.69) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $4,877.97 | ▼ $9,650.20 (-349.80) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 689 | $1.75 | $8.89 | — | $3,663.34 | ▼ $9,641.32 (-358.68) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $2,505.00 | ▼ $9,639.30 (-360.70) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 245 | $4.92 | $3.16 | — | $1,296.44 | ▼ $9,636.14 (-363.86) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $272.19 | ▼ $9,634.14 (-365.86) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1207.35 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 58 | $21.90 | $2.18 | $+73.95 | $1,540.20 | ▲ $10,206.28 (+206.28) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $2,799.52 | ▲ $10,204.10 (+204.10) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 61 | $21.17 | $2.19 | $+89.57 | $4,088.70 | ▲ $10,201.91 (+201.91) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 40 | $32.17 | $2.13 | $+97.36 | $5,373.36 | ▲ $10,199.77 (+199.77) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 689 | $1.79 | $9.01 | $+9.66 | $6,597.66 | ▲ $10,190.76 (+190.76) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $7,833.23 | ▲ $10,188.73 (+188.73) | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 245 | $5.20 | $3.21 | $+62.23 | $9,104.02 | ▲ $10,185.52 (+185.52) | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $7,907.70 | ▲ $10,183.50 (+183.50) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 75 | $17.20 | $2.21 | — | $6,615.48 | ▲ $10,181.28 (+181.28) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 116 | $11.13 | $2.34 | — | $5,322.06 | ▲ $10,178.94 (+178.94) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $4,065.89 | ▲ $10,176.89 (+176.89) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 985 | $1.32 | $12.71 | — | $2,752.98 | ▲ $10,164.18 (+164.18) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 38 | $33.36 | $2.10 | — | $1,483.20 | ▲ $10,162.08 (+162.08) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 783 | $1.66 | $10.10 | — | $173.32 | ▲ $10,151.98 (+151.98) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1300.57 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $+58.87 | $1,256.44 | ▲ $10,742.55 (+742.55) | dropped from list after 2 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $2,459.40 | ▲ $10,740.51 (+740.51) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 75 | $16.60 | $2.24 | $-49.45 | $3,702.17 | ▲ $10,738.28 (+738.28) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 116 | $13.26 | $2.37 | $+242.37 | $5,237.96 | ▲ $10,735.91 (+735.91) | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $6,470.47 | ▲ $10,733.83 (+733.83) | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 985 | $1.83 | $12.88 | $+476.76 | $8,260.14 | ▲ $10,720.95 (+720.95) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GMAB` | 38 | $32.82 | $2.12 | $-24.75 | $9,505.18 | ▲ $10,718.83 (+718.83) | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 783 | $1.55 | $10.24 | $-106.47 | $10,708.59 | ▲ $10,708.59 (+708.59) | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 55 | $24.00 | $2.15 | — | $9,386.43 | ▲ $10,706.43 (+706.43) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+13.0; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $8,093.51 | ▲ $10,704.38 (+704.38) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+9.2; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 255 | $5.23 | $3.29 | — | $6,756.57 | ▲ $10,701.09 (+701.09) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+10.7; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $5,466.85 | ▲ $10,699.09 (+699.09) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.1; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 256 | $5.22 | $3.30 | — | $4,127.23 | ▲ $10,695.79 (+695.79) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1338.57 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 90 | $14.86 | $2.26 | — | $2,787.57 | ▲ $10,693.53 (+693.53) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1338.57 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 787 | $1.70 | $10.15 | — | $1,439.51 | ▲ $10,683.37 (+683.37) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 379 | $3.53 | $4.89 | — | $96.76 | ▲ $10,678.49 (+678.49) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1338.57 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 21 | $60.07 | $2.07 | $-33.53 | $1,356.15 | ▲ $10,581.81 (+581.81) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `RZLT` | 255 | $5.01 | $3.34 | $-62.73 | $2,630.36 | ▲ $10,578.47 (+578.47) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `HCA` | 3 | $427.50 | $2.02 | $-9.24 | $3,910.84 | ▲ $10,576.45 (+576.45) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 256 | $4.98 | $3.36 | $-68.10 | $5,182.37 | ▲ $10,573.10 (+573.10) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 90 | $14.85 | $2.29 | $-5.45 | $6,516.58 | ▲ $10,570.81 (+570.81) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 787 | $1.60 | $10.29 | $-99.15 | $7,765.49 | ▲ $10,560.52 (+560.52) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 379 | $3.77 | $4.96 | $+81.11 | $9,189.35 | ▲ $10,555.55 (+555.55) | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 37 | $40.72 | $2.10 | — | $7,680.61 | ▲ $10,553.45 (+553.45) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.8; leftover $1531.56 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 108 | $14.09 | $2.31 | — | $6,156.58 | ▲ $10,551.14 (+551.14) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.1; leftover $1531.56 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 98 | $15.60 | $2.28 | — | $4,625.49 | ▲ $10,548.85 (+548.85) | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+7.1; leftover $1531.56 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 53 | $28.89 | $2.15 | — | $3,092.18 | ▲ $10,546.71 (+546.71) | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+1.6; leftover $1531.56 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `PLTR` | 8 | $170.60 | $2.01 | — | $1,725.36 | ▲ $10,544.69 (+544.69) | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+3.4; leftover $1531.56 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 98 | $15.33 | $2.31 | $-31.06 | $3,225.39 | ▲ $10,647.02 (+647.02) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GEN` | 53 | $29.83 | $2.17 | $+45.50 | $4,804.21 | ▲ $10,644.85 (+644.85) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `PLTR` | 8 | $178.75 | $2.04 | $+61.15 | $6,232.17 | ▲ $10,642.81 (+642.81) | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 903 | $1.38 | $11.65 | — | $4,974.38 | ▲ $10,631.16 (+631.16) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1246.43 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 78 | $15.94 | $2.22 | — | $3,728.84 | ▲ $10,628.94 (+628.94) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1246.43 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 4 | $272.00 | $2.00 | — | $2,638.84 | ▲ $10,626.94 (+626.94) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ⚪; ret5=-3.9; leftover $1246.43 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 528 | $2.36 | $6.81 | — | $1,385.95 | ▲ $10,620.13 (+620.13) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ret5=-2.1; leftover $1246.43 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 42 | $29.33 | $2.12 | — | $151.97 | ▲ $10,618.01 (+618.01) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1246.43 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 55 | $23.75 | $2.18 | $-18.08 | $1,456.05 | ▲ $10,329.53 (+329.53) | dropped from list after 4 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `RRC` | 37 | $41.11 | $2.12 | $+10.21 | $2,974.99 | ▲ $10,327.40 (+327.40) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CRK` | 108 | $14.56 | $2.34 | $+46.10 | $4,545.13 | ▲ $10,325.06 (+325.06) | dropped from list after 2 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 903 | $1.37 | $11.81 | $-32.49 | $5,770.43 | ▲ $10,313.25 (+313.25) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 78 | $14.32 | $2.25 | $-130.83 | $6,885.14 | ▲ $10,311.00 (+311.00) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SIMO` | 4 | $246.79 | $2.02 | $-104.86 | $7,870.28 | ▲ $10,308.98 (+308.98) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `EQ` | 528 | $2.37 | $6.91 | $-8.44 | $9,114.73 | ▲ $10,302.07 (+302.07) | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 42 | $28.27 | $2.14 | $-48.77 | $10,299.94 | ▲ $10,299.94 (+299.94) | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $49.76 | $2.06 | — | $9,053.87 | ▲ $10,297.87 (+297.87) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $7,792.45 | ▲ $10,295.85 (+295.85) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 82 | $15.70 | $2.24 | — | $6,502.81 | ▲ $10,293.61 (+293.61) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1287.49 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 56 | $22.78 | $2.16 | — | $5,224.98 | ▲ $10,291.46 (+291.46) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 78 | $16.46 | $2.22 | — | $3,938.87 | ▲ $10,289.23 (+289.23) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 1119 | $1.15 | $14.44 | — | $2,637.59 | ▲ $10,274.80 (+274.80) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1287.49 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 125 | $10.27 | $2.37 | — | $1,351.47 | ▲ $10,272.43 (+272.43) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 87 | $14.79 | $2.25 | — | $62.49 | ▲ $10,270.18 (+270.18) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+5.8; leftover $1287.49 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $1,384.95 | ▲ $10,656.80 (+656.80) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 82 | $15.45 | $2.26 | $-25.00 | $2,649.59 | ▲ $10,654.54 (+654.54) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 56 | $23.88 | $2.18 | $+57.26 | $3,984.69 | ▲ $10,652.36 (+652.36) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 78 | $16.77 | $2.25 | $+19.71 | $5,290.50 | ▲ $10,650.11 (+650.11) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SID` | 1119 | $1.36 | $14.63 | $+205.92 | $6,797.71 | ▲ $10,635.48 (+635.48) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CLYM` | 87 | $13.96 | $2.28 | $-76.74 | $8,009.96 | ▲ $10,633.21 (+633.21) | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 92 | $14.50 | $2.27 | — | $6,673.69 | ▲ $10,630.94 (+630.94) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1334.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `HQ` | 78 | $17.06 | $2.22 | — | $5,340.79 | ▲ $10,628.72 (+628.72) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+17.3; leftover $1334.99 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 262 | $5.08 | $3.38 | — | $4,006.45 | ▲ $10,625.34 (+625.34) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1334.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 494 | $2.70 | $6.37 | — | $2,666.27 | ▲ $10,618.96 (+618.96) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1334.99 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 248 | $5.37 | $3.20 | — | $1,331.31 | ▲ $10,615.76 (+615.76) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=-0.4; leftover $1334.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 99 | $13.30 | $2.29 | — | $12.33 | ▲ $10,613.48 (+613.48) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $1334.99 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
