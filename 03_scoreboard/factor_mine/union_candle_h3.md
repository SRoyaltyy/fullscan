# Factor mine action — `union_candle_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ candle, no 🚨

Cash book **-5.27%** ($9,473) · signal-only (no cash/fees) was +13.07%. Starts YES **7/17**. Fills 81 · skips 144 · realized $-640.13.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `candle_capture=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $39.05.

## Each session (cash + holdings state)

| Date | S | Open cash | Open held | Bought | Sold | Close cash | Stock | Equity | Close held | Why |
|---|---:|---:|---|---|---|---:|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | IREN, TPG, TNDM | — | $79.27 | $10,057.48 | $10,136.75 | IREN×72, TPG×65, TNDM×142 | BUY IREN x72 @ 45.98; BUY TPG x65 @ 50.62; BUY TNDM x142 @ 23.33 |
| 2026-08-14 | +5.50 | $79.27 | IREN×72, TPG×65, TNDM×142 | ANGX, QMLS | — | $63.19 | $9,861.57 | $9,924.76 | IREN×72, TPG×65, TNDM×142, ANGX×2, QMLS×1 | BUY ANGX x2 @ 4.31; BUY QMLS x1 @ 7.29 |
| 2026-08-17 | +2.25 | $63.19 | IREN×72, TPG×65, TNDM×142, ANGX×2, QMLS×1 | NPWR | — | $55.42 | $9,781.54 | $9,836.96 | IREN×72, TPG×65, TNDM×142, ANGX×2, QMLS×1, NPWR×4 | BUY NPWR x4 @ 1.92 |
| 2026-08-18 | -6.20 | $55.42 | IREN×72, TPG×65, TNDM×142, ANGX×2, QMLS×1, NPWR×4 | — | IREN, TPG, TNDM | $9,696.58 | $23.04 | $9,719.62 | ANGX×2, QMLS×1, NPWR×4 | SELL IREN (dropped from list after 3 sess (min 3)); SELL TPG (dropped from list after 3 sess (min 3)); SELL TNDM (dropped from list after 3 sess (min 3)); hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | -7.20 | $9,696.58 | ANGX×2, QMLS×1, NPWR×4 | — | ANGX, QMLS | $9,712.69 | $6.68 | $9,719.37 | NPWR×4 | SELL ANGX (dropped from list after 3 sess (min 3)); SELL QMLS (dropped from list after 3 sess (min 3)); hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | +1.12 | $9,712.69 | NPWR×4 | AG, CDE, IAG, KGC, NFGC, WPM, ABUS, AEM | NPWR | $268.59 | $9,663.84 | $9,932.43 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5 | SELL NPWR (dropped from list after 3 sess (min 3)); BUY AG x59 @ 20.55; BUY CDE x58 @ 20.65; BUY IAG x61 @ 19.63; BUY KGC x41 @ 29.63; BUY NFGC x694 @ 1.75; BUY WPM x8 @ 144.54; BUY ABUS x246 @ 4.92; BUY AEM x5 @ 204.45 |
| 2026-08-21 | +3.25 | $268.59 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5 | AUPH, ARCT, CYPH, GMAB, BTBT | — | $89.03 | $10,177.90 | $10,266.93 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5, AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23 | BUY AUPH x2 @ 17.20; BUY ARCT x3 @ 11.13; BUY CYPH x29 @ 1.32; BUY GMAB x1 @ 33.36; BUY BTBT x23 @ 1.66 |
| 2026-08-24 | -5.17 | $89.03 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5, AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23 | — | — | $89.03 | $10,159.46 | $10,248.49 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5, AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23 | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | +1.80 | $89.03 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5, AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23 | MOS, INSP, RZLT, HCA, ALVO, ALIT, GORO | AG, CDE, IAG, KGC, NFGC, WPM, ABUS, AEM | $190.67 | $10,062.69 | $10,253.36 | AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407 | SELL AG (dropped from list after 3 sess (min 3)); SELL CDE (dropped from list after 3 sess (min 3)); SELL IAG (dropped from list after 3 sess (min 3)); SELL KGC (dropped from list after 3 sess (min 3)); SELL NFGC (dropped from list after 3 sess (min 3)); SELL WPM (dropped from list after 3 sess (min 3)); SELL ABUS (dropped from list after 3 sess (min 3)); SELL AEM (dropped from list after 3 sess (min 3)); BUY MOS x59 @ 24.00; BUY INSP x23 @ 61.47; BUY RZLT x274 @ 5.23; BUY HCA x3 @ 429.24; BUY ALVO x275 @ 5.22; BUY ALIT x96 @ 14.86; BUY GORO x407 @ 3.53 |
| 2026-08-26 | +2.02 | $190.67 | AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407 | — | — | $190.67 | $10,044.20 | $10,234.87 | AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407 | hold AUPH,ARCT,CYPH,GMAB,BTBT,MOS,INSP,RZLT,HCA,ALVO,ALIT,GORO |
| 2026-08-27 | — | $190.67 | AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407 | RRC, CRK, DLO, GEN | AUPH, ARCT, CYPH, GMAB, BTBT | $163.60 | $9,904.40 | $10,068.00 | MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407, RRC×1, CRK×4, DLO×4, GEN×2 | SELL AUPH (dropped from list after 4 sess (min 3)); SELL ARCT (dropped from list after 4 sess (min 3)); SELL CYPH (dropped from list after 4 sess (min 3)); SELL GMAB (dropped from list after 4 sess (min 3)); SELL BTBT (dropped from list after 4 sess (min 3)); BUY RRC x1 @ 40.72; BUY CRK x4 @ 14.09; BUY DLO x4 @ 15.60; BUY GEN x2 @ 28.89 |
| 2026-08-28 | +0.75 | $163.60 | MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407, RRC×1, CRK×4, DLO×4, GEN×2 | LVWR, GRRR, SIMO, EQ, ZYME | INSP, RZLT, HCA, ALVO, ALIT, GORO | $54.60 | $9,863.09 | $9,917.69 | MOS×59, RRC×1, CRK×4, DLO×4, GEN×2, LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | SELL INSP (dropped from list after 3 sess (min 3)); SELL RZLT (dropped from list after 3 sess (min 3)); SELL HCA (dropped from list after 3 sess (min 3)); SELL ALVO (dropped from list after 3 sess (min 3)); SELL ALIT (dropped from list after 3 sess (min 3)); SELL GORO (dropped from list after 3 sess (min 3)); BUY LVWR x1222 @ 1.38; BUY GRRR x105 @ 15.94; BUY SIMO x6 @ 272.00; BUY EQ x714 @ 2.36; BUY ZYME x57 @ 29.33 |
| 2026-08-31 | -5.85 | $54.60 | MOS×59, RRC×1, CRK×4, DLO×4, GEN×2, LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | — | MOS | $1,453.66 | $8,134.65 | $9,588.31 | RRC×1, CRK×4, DLO×4, GEN×2, LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | SELL MOS (dropped from list after 4 sess (min 3)); hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | -6.30 | $1,453.66 | RRC×1, CRK×4, DLO×4, GEN×2, LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | — | RRC, CRK, DLO, GEN | $1,670.55 | $7,735.75 | $9,406.30 | LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | SELL RRC (dropped from list after 3 sess (min 3)); SELL CRK (dropped from list after 3 sess (min 3)); SELL DLO (dropped from list after 3 sess (min 3)); SELL GEN (dropped from list after 3 sess (min 3)); hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | -3.83 | $1,670.55 | LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | — | LVWR, GRRR, SIMO, EQ, ZYME | $9,359.89 | $0.00 | $9,359.89 | — | SELL LVWR (dropped from list after 3 sess (min 3)); SELL GRRR (dropped from list after 3 sess (min 3)); SELL SIMO (dropped from list after 3 sess (min 3)); SELL EQ (dropped from list after 3 sess (min 3)); SELL ZYME (dropped from list after 3 sess (min 3)); hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | -0.90 | $9,359.89 | — | ATRC, RVTY, CRK, MMED, ARCT, SID, NVAX, CLYM | — | $62.93 | $9,483.29 | $9,546.22 | ATRC×23, RVTY×9, CRK×74, MMED×51, ARCT×71, SID×1017, NVAX×113, CLYM×79 | BUY ATRC x23 @ 49.76; BUY RVTY x9 @ 125.94; BUY CRK x74 @ 15.70; BUY MMED x51 @ 22.78; BUY ARCT x71 @ 16.46; BUY SID x1017 @ 1.15; BUY NVAX x113 @ 10.27; BUY CLYM x79 @ 14.79 |
| 2026-09-04 | — | $62.93 | ATRC×23, RVTY×9, CRK×74, MMED×51, ARCT×71, SID×1017, NVAX×113, CLYM×79 | OABI, ALEC, UAMY | — | $39.05 | $9,433.77 | $9,472.82 | ATRC×23, RVTY×9, CRK×74, MMED×51, ARCT×71, SID×1017, NVAX×113, CLYM×79, OABI×2, ALEC×3, UAMY×1 | BUY OABI x2 @ 5.08; BUY ALEC x3 @ 2.70; BUY UAMY x1 @ 5.37 |

## Fills (what was bought / sold)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity (cash+stock) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 72 | $45.98 | $2.21 | — | $6,687.23 | ▼ $9,997.79 (-2.21) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+12.3; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 65 | $50.62 | $2.19 | — | $3,394.54 | ▼ $9,995.61 (-4.39) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+6.2; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 142 | $23.33 | $2.42 | — | $79.27 | ▼ $9,993.19 (-6.81) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+19.7; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $70.55 | ▲ $10,102.14 (+102.14) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $9.91 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 1 | $7.29 | $0.08 | — | $63.19 | ▲ $10,102.07 (+102.07) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $9.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $55.42 | ▼ $9,954.65 (-45.35) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $7.90 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 72 | $43.56 | $2.24 | $-178.69 | $3,189.50 | ▼ $9,724.50 (-275.50) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 65 | $51.77 | $2.22 | $+70.13 | $6,552.32 | ▼ $9,722.27 (-277.73) | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 142 | $22.16 | $2.46 | $-171.02 | $9,696.58 | ▼ $9,719.81 (-280.19) | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $9,706.04 | ▼ $9,719.58 (-280.42) | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 1 | $6.74 | $0.09 | $-0.72 | $9,712.69 | ▼ $9,719.49 (-280.51) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 4 | $1.64 | $0.10 | $-1.31 | $9,719.15 | ▼ $9,719.15 (-280.85) | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,504.53 | ▼ $9,716.98 (-283.02) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $7,304.67 | ▼ $9,714.82 (-285.18) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $6,105.06 | ▼ $9,712.64 (-287.36) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $4,888.12 | ▼ $9,710.53 (-289.47) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 694 | $1.75 | $8.95 | — | $3,664.67 | ▼ $9,701.58 (-298.42) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $2,506.33 | ▼ $9,699.56 (-300.44) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 246 | $4.92 | $3.17 | — | $1,292.84 | ▼ $9,696.39 (-303.61) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $268.59 | ▼ $9,694.39 (-305.61) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $233.84 | ▲ $10,272.74 (+272.74) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $200.10 | ▲ $10,272.39 (+272.39) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 29 | $1.32 | $0.47 | — | $161.35 | ▲ $10,271.92 (+271.92) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 1 | $33.36 | $0.34 | — | $127.66 | ▲ $10,271.59 (+271.59) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 23 | $1.66 | $0.45 | — | $89.03 | ▲ $10,271.14 (+271.14) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 59 | $20.73 | $2.19 | $+6.27 | $1,309.91 | ▲ $10,278.58 (+278.58) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 58 | $20.85 | $2.18 | $+7.25 | $2,517.02 | ▲ $10,276.39 (+276.39) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 61 | $21.63 | $2.19 | $+117.63 | $3,834.26 | ▲ $10,274.20 (+274.20) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 41 | $32.76 | $2.13 | $+124.08 | $5,175.29 | ▲ $10,272.07 (+272.07) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 694 | $1.91 | $9.08 | $+93.01 | $6,491.75 | ▲ $10,262.99 (+262.99) | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $7,769.71 | ▲ $10,260.95 (+260.95) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 246 | $5.26 | $3.22 | $+77.24 | $9,060.45 | ▲ $10,257.73 (+257.73) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AEM` | 5 | $200.48 | $2.02 | $-23.88 | $10,060.83 | ▲ $10,255.71 (+255.71) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 59 | $24.00 | $2.17 | — | $8,642.66 | ▲ $10,253.54 (+253.54) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+13.0; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 23 | $61.47 | $2.06 | — | $7,226.79 | ▲ $10,251.48 (+251.48) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+9.2; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 274 | $5.23 | $3.53 | — | $5,790.23 | ▲ $10,247.94 (+247.94) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+10.7; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $4,500.52 | ▲ $10,245.95 (+245.95) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.1; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 275 | $5.22 | $3.55 | — | $3,061.47 | ▲ $10,242.40 (+242.40) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1437.26 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 96 | $14.86 | $2.28 | — | $1,632.63 | ▲ $10,240.12 (+240.12) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1437.26 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 407 | $3.53 | $5.25 | — | $190.67 | ▲ $10,234.87 (+234.87) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $223.51 | ▲ $10,216.83 (+216.83) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 3 | $15.35 | $0.49 | $+11.83 | $269.07 | ▲ $10,216.34 (+216.34) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 29 | $1.60 | $0.57 | $+7.08 | $314.90 | ▲ $10,215.77 (+215.77) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `GMAB` | 1 | $33.78 | $0.36 | $-0.28 | $348.32 | ▲ $10,215.41 (+215.41) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 23 | $1.53 | $0.44 | $-3.88 | $383.07 | ▲ $10,214.97 (+214.97) | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $341.94 | ▲ $10,214.56 (+214.56) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.8; leftover $63.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 4 | $14.09 | $0.58 | — | $285.00 | ▲ $10,213.98 (+213.98) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.1; leftover $63.84 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 4 | $15.60 | $0.64 | — | $221.97 | ▲ $10,213.35 (+213.35) | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+7.1; leftover $63.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 2 | $28.89 | $0.58 | — | $163.60 | ▲ $10,212.76 (+212.76) | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+1.6; leftover $63.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 23 | $62.10 | $2.08 | $+10.35 | $1,589.82 | ▲ $10,087.90 (+87.90) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 274 | $5.07 | $3.59 | $-50.97 | $2,975.41 | ▲ $10,084.31 (+84.31) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $4,247.22 | ▲ $10,082.29 (+82.29) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 275 | $4.88 | $3.60 | $-100.65 | $5,585.62 | ▲ $10,078.69 (+78.69) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 96 | $14.54 | $2.31 | $-35.30 | $6,979.15 | ▲ $10,076.38 (+76.38) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 407 | $3.59 | $5.33 | $+13.84 | $8,434.95 | ▲ $10,071.05 (+71.05) | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1222 | $1.38 | $15.76 | — | $6,732.83 | ▲ $10,055.29 (+55.29) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1686.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 105 | $15.94 | $2.31 | — | $5,056.83 | ▲ $10,052.99 (+52.99) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1686.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 6 | $272.00 | $2.01 | — | $3,422.82 | ▲ $10,050.98 (+50.98) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ⚪; ret5=-3.9; leftover $1686.99 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 714 | $2.36 | $9.21 | — | $1,728.57 | ▲ $10,041.77 (+41.77) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ret5=-2.1; leftover $1686.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 57 | $29.33 | $2.16 | — | $54.60 | ▲ $10,039.61 (+39.61) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1686.99 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 59 | $23.75 | $2.19 | $-19.11 | $1,453.66 | ▼ $9,637.14 (-362.86) | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $1,494.54 | ▼ $9,520.71 (-479.29) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 4 | $14.31 | $0.60 | $-0.30 | $1,551.18 | ▼ $9,520.11 (-479.89) | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 4 | $14.88 | $0.63 | $-4.14 | $1,610.07 | ▼ $9,519.48 (-480.52) | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 2 | $30.56 | $0.64 | $+2.12 | $1,670.55 | ▼ $9,518.84 (-481.16) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1222 | $1.19 | $15.98 | $-263.92 | $3,108.75 | ▼ $9,375.78 (-624.22) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 105 | $14.75 | $2.33 | $-129.59 | $4,655.17 | ▼ $9,373.45 (-626.55) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 6 | $240.09 | $2.03 | $-195.50 | $6,093.68 | ▼ $9,371.42 (-628.58) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 714 | $2.25 | $9.34 | $-97.09 | $7,690.84 | ▼ $9,362.08 (-637.92) | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 57 | $29.32 | $2.18 | $-4.92 | $9,359.89 | ▼ $9,359.89 (-640.11) | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $49.76 | $2.06 | — | $8,213.35 | ▼ $9,357.83 (-642.17) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $7,077.88 | ▼ $9,355.82 (-644.18) | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 74 | $15.70 | $2.21 | — | $5,913.87 | ▼ $9,353.61 (-646.39) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1169.99 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $4,749.94 | ▼ $9,351.46 (-648.54) | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.46 | $2.20 | — | $3,579.08 | ▼ $9,349.26 (-650.74) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 1017 | $1.15 | $13.12 | — | $2,396.41 | ▼ $9,336.14 (-663.86) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1169.99 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 113 | $10.27 | $2.33 | — | $1,233.57 | ▼ $9,333.81 (-666.19) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 79 | $14.79 | $2.23 | — | $62.93 | ▼ $9,331.58 (-668.42) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+5.8; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $5.08 | $0.11 | — | $52.67 | ▼ $9,685.26 (-314.74) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $10.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 3 | $2.70 | $0.09 | — | $44.48 | ▼ $9,685.17 (-314.83) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $10.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 1 | $5.37 | $0.06 | — | $39.05 | ▼ $9,685.11 (-314.89) | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=-0.4; leftover $10.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `SLG` | cash | leftover split 9.91 < 1 share @ 57.61 |
| 2026-08-14 | `WDC` | cash | leftover split 9.91 < 1 share @ 503.50 |
| 2026-08-14 | `ADUR` | cash | leftover split 9.91 < 1 share @ 16.50 |
| 2026-08-14 | `ARX` | cash | leftover split 9.91 < 1 share @ 19.57 |
| 2026-08-14 | `AIRO` | cash | leftover split 9.91 < 1 share @ 11.12 |
| 2026-08-14 | `TBBB` | cash | leftover split 9.91 < 1 share @ 48.82 |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 7.90 < 1 share @ 46.18 |
| 2026-08-17 | `FANG` | cash | leftover split 7.90 < 1 share @ 202.70 |
| 2026-08-17 | `CDNL` | cash | leftover split 7.90 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 7.90 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 7.90 < 1 share @ 31.30 |
| 2026-08-17 | `HTFL` | cash | leftover split 7.90 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 7.90 < 1 share @ 32.55 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 38.37 < 1 share @ 119.43 |
| 2026-08-21 | `CRSP` | cash | leftover split 38.37 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GMAB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GMAB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GMAB` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `INDP` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASML` | cash | leftover split 63.84 < 1 share @ 1746.33 |
| 2026-08-27 | `PLTR` | cash | leftover split 63.84 < 1 share @ 170.60 |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-28 | `GEN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `GEN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SIMO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `EQ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FROG` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SIMO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `EQ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SID` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BVS` | cash | leftover split 10.49 < 1 share @ 14.50 |
| 2026-09-04 | `HQ` | cash | leftover split 10.49 < 1 share @ 17.06 |
| 2026-09-04 | `FMC` | cash | leftover split 10.49 < 1 share @ 13.30 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 23 | 2026-09-03 @ $49.76 | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1169.99 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1169.99 |
| `CRK` | 74 | 2026-09-03 @ $15.70 | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1169.99 |
| `MMED` | 51 | 2026-09-03 @ $22.78 | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1169.99 |
| `ARCT` | 71 | 2026-09-03 @ $16.46 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1169.99 |
| `SID` | 1017 | 2026-09-03 @ $1.15 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1169.99 |
| `NVAX` | 113 | 2026-09-03 @ $10.27 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1169.99 |
| `CLYM` | 79 | 2026-09-03 @ $14.79 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+5.8; leftover $1169.99 |
| `OABI` | 2 | 2026-09-04 @ $5.08 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $10.49 |
| `ALEC` | 3 | 2026-09-04 @ $2.70 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $10.49 |
| `UAMY` | 1 | 2026-09-04 @ $5.37 | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=-0.4; leftover $10.49 |
