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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | IREN, TPG, TNDM | — | $79.27 | $10,136.75 | IREN×72, TPG×65, TNDM×142 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $79.27 | IREN×72, TPG×65, TNDM×142 | $10,102.24 | -34.51 | ANGX, QMLS | — | $63.19 | $9,924.76 | IREN×72, TPG×65, TNDM×142, ANGX×2, QMLS×1 | 09:30 open · cash $79.27 (unchanged overnight, no fees) · equity $10,102.24 vs prior close $10,136.75 (-34.51) because holdings re-marked: IREN×72 yday $44.76 → 09:30 $44.09 -48.24; TPG×65 yday $54.62 → 09:30 $55.29 +43.55; TNDM×142 yday $23.13 → 09:30 $22.92 -29.82 |
| 2026-08-17 | +2.25 | $63.19 | IREN×72, TPG×65, TNDM×142, ANGX×2, QMLS×1 | $9,954.74 | +29.98 | NPWR | — | $55.42 | $9,836.96 | IREN×72, TPG×65, TNDM×142, ANGX×2, QMLS×1, NPWR×4 | 09:30 open · cash $63.19 (unchanged overnight, no fees) · equity $9,954.74 vs prior close $9,924.76 (+29.98) because holdings re-marked: IREN×72 yday $44.06 → 09:30 $45.23 +84.24; TPG×65 yday $53.03 → 09:30 $52.67 -23.40; TNDM×142 yday $22.72 → 09:30 $22.50 -31.24; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46; QMLS×1 yday $7.32 → 09:30 $7.24 -0.08 |
| 2026-08-18 | -6.20 | $55.42 | IREN×72, TPG×65, TNDM×142, ANGX×2, QMLS×1, NPWR×4 | $9,726.74 | -110.22 | — | IREN, TPG, TNDM | $9,696.58 | $9,719.62 | ANGX×2, QMLS×1, NPWR×4 | 09:30 open · cash $55.42 (unchanged overnight, no fees) · equity $9,726.74 vs prior close $9,836.96 (-110.22) because holdings re-marked: IREN×72 yday $44.90 → 09:30 $43.56 -96.48; TPG×65 yday $51.77 → 09:30 $51.77 +0.00; TNDM×142 yday $22.25 → 09:30 $22.16 -13.49; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; QMLS×1 yday $7.14 → 09:30 $6.85 -0.29; NPWR×4 yday $1.73 → 09:30 $1.70 -0.12 |
| 2026-08-19 | -7.20 | $9,696.58 | ANGX×2, QMLS×1, NPWR×4 | $9,719.70 | +0.08 | — | ANGX, QMLS | $9,712.69 | $9,719.37 | NPWR×4 | 09:30 open · cash $9,696.58 (unchanged overnight, no fees) · equity $9,719.70 vs prior close $9,719.62 (+0.08) because holdings re-marked: ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; QMLS×1 yday $6.74 → 09:30 $6.74 +0.00; NPWR×4 yday $1.65 → 09:30 $1.70 +0.20 |
| 2026-08-20 | +1.12 | $9,712.69 | NPWR×4 | $9,719.25 | -0.12 | AG, CDE, IAG, KGC, NFGC, WPM, ABUS, AEM | NPWR | $268.59 | $9,932.43 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5 | 09:30 open · cash $9,712.69 (unchanged overnight, no fees) · equity $9,719.25 vs prior close $9,719.37 (-0.12) because holdings re-marked: NPWR×4 yday $1.67 → 09:30 $1.64 -0.12 |
| 2026-08-21 | +3.25 | $268.59 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5 | $10,273.09 | +340.66 | AUPH, ARCT, CYPH, GMAB, BTBT | — | $89.03 | $10,266.93 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5, AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23 | 09:30 open · cash $268.59 (unchanged overnight, no fees) · equity $10,273.09 vs prior close $9,932.43 (+340.66) because holdings re-marked: AG×59 yday $21.19 → 09:30 $21.90 +41.89; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×694 yday $1.75 → 09:30 $1.79 +27.76; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×246 yday $4.77 → 09:30 $5.20 +105.78; AEM×5 yday $212.04 → 09:30 $216.30 +21.30 |
| 2026-08-24 | -5.17 | $89.03 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5, AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23 | $10,374.76 | +107.83 | — | — | $89.03 | $10,248.49 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5, AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23 | 09:30 open · cash $89.03 (unchanged overnight, no fees) · equity $10,374.76 vs prior close $10,266.93 (+107.83) because holdings re-marked: AG×59 yday $21.09 → 09:30 $21.47 +22.42; CDE×58 yday $20.97 → 09:30 $21.26 +16.82; IAG×61 yday $21.14 → 09:30 $21.44 +18.30; KGC×41 yday $32.76 → 09:30 $33.21 +18.45; NFGC×694 yday $1.84 → 09:30 $1.86 +13.88; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×246 yday $5.21 → 09:30 $5.18 -7.38; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; AUPH×2 yday $16.65 → 09:30 $16.60 -0.10; ARCT×3 yday $13.45 → 09:30 $13.26 -0.57; CYPH×29 yday $1.42 → 09:30 $1.83 +11.89; GMAB×1 yday $33.45 → 09:30 $32.82 -0.63; BTBT×23 yday $1.53 → 09:30 $1.55 +0.46 |
| 2026-08-25 | +1.80 | $89.03 | AG×59, CDE×58, IAG×61, KGC×41, NFGC×694, WPM×8, ABUS×246, AEM×5, AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23 | $10,280.77 | +32.28 | MOS, INSP, RZLT, HCA, ALVO, ALIT, GORO | AG, CDE, IAG, KGC, NFGC, WPM, ABUS, AEM | $190.67 | $10,253.36 | AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407 | 09:30 open · cash $89.03 (unchanged overnight, no fees) · equity $10,280.77 vs prior close $10,248.49 (+32.28) because holdings re-marked: AG×59 yday $20.57 → 09:30 $20.73 +9.44; CDE×58 yday $20.49 → 09:30 $20.85 +20.88; IAG×61 yday $21.36 → 09:30 $21.63 +16.47; KGC×41 yday $32.47 → 09:30 $32.76 +11.89; NFGC×694 yday $1.90 → 09:30 $1.91 +6.94; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×246 yday $5.20 → 09:30 $5.26 +14.76; AEM×5 yday $214.08 → 09:30 $200.48 -68.00; AUPH×2 yday $16.60 → 09:30 $16.71 +0.22; ARCT×3 yday $13.76 → 09:30 $14.34 +1.74; CYPH×29 yday $1.64 → 09:30 $1.70 +1.74; GMAB×1 yday $33.06 → 09:30 $33.49 +0.43; BTBT×23 yday $1.56 → 09:30 $1.55 -0.23 |
| 2026-08-26 | +2.02 | $190.67 | AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407 | $10,253.36 | -0.00 | — | — | $190.67 | $10,234.87 | AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407 | 09:30 open · cash $190.67 (unchanged overnight, no fees) · equity $10,253.36 vs prior close $10,253.36 (-0.00) because holdings re-marked: AUPH×2 yday $16.71 → 09:30 $16.71 +0.00; ARCT×3 yday $14.21 → 09:30 $14.21 +0.00; CYPH×29 yday $1.64 → 09:30 $1.64 +0.00; GMAB×1 yday $33.68 → 09:30 $33.68 +0.00; BTBT×23 yday $1.53 → 09:30 $1.53 +0.00; MOS×59 yday $23.75 → 09:30 $23.75 +0.00; INSP×23 yday $61.47 → 09:30 $61.47 +0.00; RZLT×274 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALVO×275 yday $5.25 → 09:30 $5.25 +0.00; ALIT×96 yday $14.87 → 09:30 $14.87 +0.00; GORO×407 yday $3.56 → 09:30 $3.56 +0.00 |
| 2026-08-27 | — | $190.67 | AUPH×2, ARCT×3, CYPH×29, GMAB×1, BTBT×23, MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407 | $10,217.19 | -17.68 | RRC, CRK, DLO, GEN | AUPH, ARCT, CYPH, GMAB, BTBT | $163.60 | $10,068.00 | MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407, RRC×1, CRK×4, DLO×4, GEN×2 | 09:30 open · cash $190.67 (unchanged overnight, no fees) · equity $10,217.19 vs prior close $10,234.87 (-17.68) because holdings re-marked: AUPH×2 yday $16.71 → 09:30 $16.60 -0.22; ARCT×3 yday $14.21 → 09:30 $15.35 +3.42; CYPH×29 yday $1.64 → 09:30 $1.60 -1.16; GMAB×1 yday $33.68 → 09:30 $33.78 +0.10; BTBT×23 yday $1.53 → 09:30 $1.53 +0.00; MOS×59 yday $23.75 → 09:30 $24.84 +64.31; INSP×23 yday $61.47 → 09:30 $60.07 -32.20; RZLT×274 yday $5.29 → 09:30 $5.01 -76.72; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALVO×275 yday $5.25 → 09:30 $4.98 -74.25; ALIT×96 yday $14.87 → 09:30 $14.85 -1.92; GORO×407 yday $3.56 → 09:30 $3.77 +85.47 |
| 2026-08-28 | +0.75 | $163.60 | MOS×59, INSP×23, RZLT×274, HCA×3, ALVO×275, ALIT×96, GORO×407, RRC×1, CRK×4, DLO×4, GEN×2 | $10,089.98 | +21.98 | LVWR, GRRR, SIMO, EQ, ZYME | INSP, RZLT, HCA, ALVO, ALIT, GORO | $54.60 | $9,917.69 | MOS×59, RRC×1, CRK×4, DLO×4, GEN×2, LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | 09:30 open · cash $163.60 (unchanged overnight, no fees) · equity $10,089.98 vs prior close $10,068.00 (+21.98) because holdings re-marked: MOS×59 yday $24.16 → 09:30 $24.00 -9.44; INSP×23 yday $61.80 → 09:30 $62.10 +6.90; RZLT×274 yday $5.04 → 09:30 $5.07 +8.22; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; ALVO×275 yday $4.91 → 09:30 $4.88 -8.25; ALIT×96 yday $14.33 → 09:30 $14.54 +20.16; GORO×407 yday $3.56 → 09:30 $3.59 +12.21; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; CRK×4 yday $14.50 → 09:30 $14.42 -0.32; DLO×4 yday $15.36 → 09:30 $15.33 -0.12; GEN×2 yday $29.64 → 09:30 $29.83 +0.38 |
| 2026-08-31 | -5.85 | $54.60 | MOS×59, RRC×1, CRK×4, DLO×4, GEN×2, LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | $9,639.33 | -278.36 | — | MOS | $1,453.66 | $9,588.31 | RRC×1, CRK×4, DLO×4, GEN×2, LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | 09:30 open · cash $54.60 (unchanged overnight, no fees) · equity $9,639.33 vs prior close $9,917.69 (-278.36) because holdings re-marked: MOS×59 yday $23.76 → 09:30 $23.75 -0.59; RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×4 yday $14.62 → 09:30 $14.56 -0.24; DLO×4 yday $15.14 → 09:30 $15.01 -0.52; GEN×2 yday $30.50 → 09:30 $31.02 +1.04; LVWR×1222 yday $1.36 → 09:30 $1.37 +12.22; GRRR×105 yday $15.66 → 09:30 $14.32 -140.70; SIMO×6 yday $255.08 → 09:30 $246.79 -49.74; EQ×714 yday $2.45 → 09:30 $2.37 -57.12; ZYME×57 yday $29.01 → 09:30 $28.27 -42.18 |
| 2026-09-01 | -6.30 | $1,453.66 | RRC×1, CRK×4, DLO×4, GEN×2, LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | $9,521.15 | -67.16 | — | RRC, CRK, DLO, GEN | $1,670.55 | $9,406.30 | LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | 09:30 open · cash $1,453.66 (unchanged overnight, no fees) · equity $9,521.15 vs prior close $9,588.31 (-67.16) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×4 yday $14.51 → 09:30 $14.31 -0.80; DLO×4 yday $15.00 → 09:30 $14.88 -0.48; GEN×2 yday $31.02 → 09:30 $30.56 -0.92; LVWR×1222 yday $1.34 → 09:30 $1.22 -146.64; GRRR×105 yday $14.20 → 09:30 $15.05 +89.25; SIMO×6 yday $246.79 → 09:30 $247.53 +4.44; EQ×714 yday $2.37 → 09:30 $2.27 -71.40; ZYME×57 yday $28.27 → 09:30 $29.32 +59.85 |
| 2026-09-02 | -3.83 | $1,670.55 | LVWR×1222, GRRR×105, SIMO×6, EQ×714, ZYME×57 | $9,391.76 | -14.54 | — | LVWR, GRRR, SIMO, EQ, ZYME | $9,359.89 | $9,359.89 | — | 09:30 open · cash $1,670.55 (unchanged overnight, no fees) · equity $9,391.76 vs prior close $9,406.30 (-14.54) because holdings re-marked: LVWR×1222 yday $1.18 → 09:30 $1.19 +12.22; GRRR×105 yday $14.80 → 09:30 $14.75 -5.25; SIMO×6 yday $241.20 → 09:30 $240.09 -6.66; EQ×714 yday $2.27 → 09:30 $2.25 -14.28; ZYME×57 yday $29.33 → 09:30 $29.32 -0.57 |
| 2026-09-03 | -0.90 | $9,359.89 | — | $9,359.89 | +0.00 | ATRC, RVTY, CRK, MMED, ARCT, SID, NVAX, CLYM | — | $62.93 | $9,546.22 | ATRC×23, RVTY×9, CRK×74, MMED×51, ARCT×71, SID×1017, NVAX×113, CLYM×79 | 09:30 open · cash $9,359.89 · no holdings · equity $9,359.89 vs prior close $9,359.89 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $62.93 | ATRC×23, RVTY×9, CRK×74, MMED×51, ARCT×71, SID×1017, NVAX×113, CLYM×79 | $9,685.36 | +139.14 | OABI, ALEC, UAMY | — | $39.05 | $9,472.82 | ATRC×23, RVTY×9, CRK×74, MMED×51, ARCT×71, SID×1017, NVAX×113, CLYM×79, OABI×2, ALEC×3, UAMY×1 | 09:30 open · cash $62.93 (unchanged overnight, no fees) · equity $9,685.36 vs prior close $9,546.22 (+139.14) because holdings re-marked: ATRC×23 yday $52.59 → 09:30 $52.88 +6.67; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×74 yday $15.54 → 09:30 $15.45 -6.66; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; ARCT×71 yday $16.74 → 09:30 $16.77 +2.13; SID×1017 yday $1.17 → 09:30 $1.36 +193.23; NVAX×113 yday $10.32 → 09:30 $10.41 +10.17; CLYM×79 yday $15.05 → 09:30 $13.96 -86.11 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 72 | $45.98 | $2.21 | — | $6,687.23 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+12.3; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 65 | $50.62 | $2.19 | — | $3,394.54 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+6.2; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 142 | $23.33 | $2.42 | — | $79.27 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+19.7; leftover $3333.33 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $79.27 | ▼ 09:30 equity $10,102.24 vs yday $10,136.75 (-34.51) | 09:30 open · cash $79.27 (unchanged overnight, no fees) · equity $10,102.24 vs prior close $10,136.75 (-34.51) because holdings re-marked: IREN×72 yday $44.76 → 09:30 $44.09 -48.24; TPG×65 yday $54.62 → 09:30 $55.29 +43.55; TNDM×142 yday $23.13 → 09:30 $22.92 -29.82 | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 2 | $4.31 | $0.09 | — | $70.55 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $9.91 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 1 | $7.29 | $0.08 | — | $63.19 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $9.91 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $63.19 | ▲ 09:30 equity $9,954.74 vs yday $9,924.76 (+29.98) | 09:30 open · cash $63.19 (unchanged overnight, no fees) · equity $9,954.74 vs prior close $9,924.76 (+29.98) because holdings re-marked: IREN×72 yday $44.06 → 09:30 $45.23 +84.24; TPG×65 yday $53.03 → 09:30 $52.67 -23.40; TNDM×142 yday $22.72 → 09:30 $22.50 -31.24; ANGX×2 yday $4.37 → 09:30 $4.60 +0.46; QMLS×1 yday $7.32 → 09:30 $7.24 -0.08 | — |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 4 | $1.92 | $0.09 | — | $55.42 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $7.90 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.42 | ▼ 09:30 equity $9,726.74 vs yday $9,836.96 (-110.22) | 09:30 open · cash $55.42 (unchanged overnight, no fees) · equity $9,726.74 vs prior close $9,836.96 (-110.22) because holdings re-marked: IREN×72 yday $44.90 → 09:30 $43.56 -96.48; TPG×65 yday $51.77 → 09:30 $51.77 +0.00; TNDM×142 yday $22.25 → 09:30 $22.16 -13.49; ANGX×2 yday $4.71 → 09:30 $4.79 +0.16; QMLS×1 yday $7.14 → 09:30 $6.85 -0.29; NPWR×4 yday $1.73 → 09:30 $1.70 -0.12 | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 72 | $43.56 | $2.24 | $-178.69 | $3,189.50 | ▼ -178.69 after sell → book $9,724.50; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 65 | $51.77 | $2.22 | $+70.13 | $6,552.32 | ▲ +70.13 after sell → book $9,722.27; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 142 | $22.16 | $2.46 | $-171.02 | $9,696.58 | ▼ -171.02 after sell → book $9,719.81; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,696.58 | ▲ 09:30 equity $9,719.70 vs yday $9,719.62 (+0.08) | 09:30 open · cash $9,696.58 (unchanged overnight, no fees) · equity $9,719.70 vs prior close $9,719.62 (+0.08) because holdings re-marked: ANGX×2 yday $4.85 → 09:30 $4.79 -0.12; QMLS×1 yday $6.74 → 09:30 $6.74 +0.00; NPWR×4 yday $1.65 → 09:30 $1.70 +0.20 | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 2 | $4.79 | $0.12 | $+0.75 | $9,706.04 | ▲ +0.75 after sell → book $9,719.58; vs 09:30 mark -0.12 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 1 | $6.74 | $0.09 | $-0.72 | $9,712.69 | ▼ -0.72 after sell → book $9,719.49; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,712.69 | ▼ 09:30 equity $9,719.25 vs yday $9,719.37 (-0.12) | 09:30 open · cash $9,712.69 (unchanged overnight, no fees) · equity $9,719.25 vs prior close $9,719.37 (-0.12) because holdings re-marked: NPWR×4 yday $1.67 → 09:30 $1.64 -0.12 | — |
| 2026-08-20 09:30 ET | **SELL** | `NPWR` | 4 | $1.64 | $0.10 | $-1.31 | $9,719.15 | ▼ -1.31 after sell → book $9,719.15; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,504.53 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $7,304.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 61 | $19.63 | $2.17 | — | $6,105.06 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $4,888.12 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 694 | $1.75 | $8.95 | — | $3,664.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $2,506.33 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 246 | $4.92 | $3.17 | — | $1,292.84 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `AEM` | 5 | $204.45 | $2.00 | — | $268.59 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,ohlc_hot,mover_buy; 🔵; ret5=+12.2; leftover $1214.89 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $268.59 | ▲ 09:30 equity $10,273.09 vs yday $9,932.43 (+340.66) | 09:30 open · cash $268.59 (unchanged overnight, no fees) · equity $10,273.09 vs prior close $9,932.43 (+340.66) because holdings re-marked: AG×59 yday $21.19 → 09:30 $21.90 +41.89; CDE×58 yday $21.11 → 09:30 $21.75 +37.12; IAG×61 yday $20.50 → 09:30 $21.17 +40.87; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×694 yday $1.75 → 09:30 $1.79 +27.76; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×246 yday $4.77 → 09:30 $5.20 +105.78; AEM×5 yday $212.04 → 09:30 $216.30 +21.30 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 2 | $17.20 | $0.35 | — | $233.84 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 3 | $11.13 | $0.34 | — | $200.10 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 29 | $1.32 | $0.47 | — | $161.35 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GMAB` | 1 | $33.36 | $0.34 | — | $127.66 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.6; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 23 | $1.66 | $0.45 | — | $89.03 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $38.37 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.03 | ▲ 09:30 equity $10,374.76 vs yday $10,266.93 (+107.83) | 09:30 open · cash $89.03 (unchanged overnight, no fees) · equity $10,374.76 vs prior close $10,266.93 (+107.83) because holdings re-marked: AG×59 yday $21.09 → 09:30 $21.47 +22.42; CDE×58 yday $20.97 → 09:30 $21.26 +16.82; IAG×61 yday $21.14 → 09:30 $21.44 +18.30; KGC×41 yday $32.76 → 09:30 $33.21 +18.45; NFGC×694 yday $1.84 → 09:30 $1.86 +13.88; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×246 yday $5.21 → 09:30 $5.18 -7.38; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; AUPH×2 yday $16.65 → 09:30 $16.60 -0.10; ARCT×3 yday $13.45 → 09:30 $13.26 -0.57; CYPH×29 yday $1.42 → 09:30 $1.83 +11.89; GMAB×1 yday $33.45 → 09:30 $32.82 -0.63; BTBT×23 yday $1.53 → 09:30 $1.55 +0.46 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $89.03 | ▲ 09:30 equity $10,280.77 vs yday $10,248.49 (+32.28) | 09:30 open · cash $89.03 (unchanged overnight, no fees) · equity $10,280.77 vs prior close $10,248.49 (+32.28) because holdings re-marked: AG×59 yday $20.57 → 09:30 $20.73 +9.44; CDE×58 yday $20.49 → 09:30 $20.85 +20.88; IAG×61 yday $21.36 → 09:30 $21.63 +16.47; KGC×41 yday $32.47 → 09:30 $32.76 +11.89; NFGC×694 yday $1.90 → 09:30 $1.91 +6.94; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×246 yday $5.20 → 09:30 $5.26 +14.76; AEM×5 yday $214.08 → 09:30 $200.48 -68.00; AUPH×2 yday $16.60 → 09:30 $16.71 +0.22; ARCT×3 yday $13.76 → 09:30 $14.34 +1.74; CYPH×29 yday $1.64 → 09:30 $1.70 +1.74; GMAB×1 yday $33.06 → 09:30 $33.49 +0.43; BTBT×23 yday $1.56 → 09:30 $1.55 -0.23 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 59 | $20.73 | $2.19 | $+6.27 | $1,309.91 | ▲ +6.27 after sell → book $10,278.58; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 58 | $20.85 | $2.18 | $+7.25 | $2,517.02 | ▲ +7.25 after sell → book $10,276.39; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 61 | $21.63 | $2.19 | $+117.63 | $3,834.26 | ▲ +117.63 after sell → book $10,274.20; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 41 | $32.76 | $2.13 | $+124.08 | $5,175.29 | ▲ +124.08 after sell → book $10,272.07; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 694 | $1.91 | $9.08 | $+93.01 | $6,491.75 | ▲ +93.01 after sell → book $10,262.99; vs 09:30 mark -9.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $7,769.71 | ▲ +119.63 after sell → book $10,260.95; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 246 | $5.26 | $3.22 | $+77.24 | $9,060.45 | ▲ +77.24 after sell → book $10,257.73; vs 09:30 mark -3.22 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `AEM` | 5 | $200.48 | $2.02 | $-23.88 | $10,060.83 | ▼ -23.88 after sell → book $10,255.71; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 59 | $24.00 | $2.17 | — | $8,642.66 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ⚪; ret5=+13.0; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 23 | $61.47 | $2.06 | — | $7,226.79 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ret5=+9.2; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 274 | $5.23 | $3.53 | — | $5,790.23 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+10.7; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $4,500.52 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+6.1; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 275 | $5.22 | $3.55 | — | $3,061.47 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1437.26 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 96 | $14.86 | $2.28 | — | $1,632.63 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1437.26 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 407 | $3.53 | $5.25 | — | $190.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1437.26 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.67 | ▲ 09:30 equity $10,253.36 vs yday $10,253.36 (-0.00) | 09:30 open · cash $190.67 (unchanged overnight, no fees) · equity $10,253.36 vs prior close $10,253.36 (-0.00) because holdings re-marked: AUPH×2 yday $16.71 → 09:30 $16.71 +0.00; ARCT×3 yday $14.21 → 09:30 $14.21 +0.00; CYPH×29 yday $1.64 → 09:30 $1.64 +0.00; GMAB×1 yday $33.68 → 09:30 $33.68 +0.00; BTBT×23 yday $1.53 → 09:30 $1.53 +0.00; MOS×59 yday $23.75 → 09:30 $23.75 +0.00; INSP×23 yday $61.47 → 09:30 $61.47 +0.00; RZLT×274 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; ALVO×275 yday $5.25 → 09:30 $5.25 +0.00; ALIT×96 yday $14.87 → 09:30 $14.87 +0.00; GORO×407 yday $3.56 → 09:30 $3.56 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $190.67 | ▼ 09:30 equity $10,217.19 vs yday $10,234.87 (-17.68) | 09:30 open · cash $190.67 (unchanged overnight, no fees) · equity $10,217.19 vs prior close $10,234.87 (-17.68) because holdings re-marked: AUPH×2 yday $16.71 → 09:30 $16.60 -0.22; ARCT×3 yday $14.21 → 09:30 $15.35 +3.42; CYPH×29 yday $1.64 → 09:30 $1.60 -1.16; GMAB×1 yday $33.68 → 09:30 $33.78 +0.10; BTBT×23 yday $1.53 → 09:30 $1.53 +0.00; MOS×59 yday $23.75 → 09:30 $24.84 +64.31; INSP×23 yday $61.47 → 09:30 $60.07 -32.20; RZLT×274 yday $5.29 → 09:30 $5.01 -76.72; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; ALVO×275 yday $5.25 → 09:30 $4.98 -74.25; ALIT×96 yday $14.87 → 09:30 $14.85 -1.92; GORO×407 yday $3.56 → 09:30 $3.77 +85.47 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 2 | $16.60 | $0.36 | $-1.91 | $223.51 | ▼ -1.91 after sell → book $10,216.83; vs 09:30 mark -0.36 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 3 | $15.35 | $0.49 | $+11.83 | $269.07 | ▲ +11.83 after sell → book $10,216.34; vs 09:30 mark -0.49 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 29 | $1.60 | $0.57 | $+7.08 | $314.90 | ▲ +7.08 after sell → book $10,215.77; vs 09:30 mark -0.57 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `GMAB` | 1 | $33.78 | $0.36 | $-0.28 | $348.32 | ▼ -0.28 after sell → book $10,215.41; vs 09:30 mark -0.36 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 23 | $1.53 | $0.44 | $-3.88 | $383.07 | ▼ -3.88 after sell → book $10,214.97; vs 09:30 mark -0.44 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $341.94 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.8; leftover $63.84 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 4 | $14.09 | $0.58 | — | $285.00 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; ret5=+1.1; leftover $63.84 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 4 | $15.60 | $0.64 | — | $221.97 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+7.1; leftover $63.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 2 | $28.89 | $0.58 | — | $163.60 | — | union ∩ candle, no 🚨; gate candle_capture=True; list mover_buy; 🔵; ret5=+1.6; leftover $63.84 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $163.60 | ▲ 09:30 equity $10,089.98 vs yday $10,068.00 (+21.98) | 09:30 open · cash $163.60 (unchanged overnight, no fees) · equity $10,089.98 vs prior close $10,068.00 (+21.98) because holdings re-marked: MOS×59 yday $24.16 → 09:30 $24.00 -9.44; INSP×23 yday $61.80 → 09:30 $62.10 +6.90; RZLT×274 yday $5.04 → 09:30 $5.07 +8.22; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; ALVO×275 yday $4.91 → 09:30 $4.88 -8.25; ALIT×96 yday $14.33 → 09:30 $14.54 +20.16; GORO×407 yday $3.56 → 09:30 $3.59 +12.21; RRC×1 yday $41.55 → 09:30 $41.44 -0.11; CRK×4 yday $14.50 → 09:30 $14.42 -0.32; DLO×4 yday $15.36 → 09:30 $15.33 -0.12; GEN×2 yday $29.64 → 09:30 $29.83 +0.38 | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 23 | $62.10 | $2.08 | $+10.35 | $1,589.82 | ▲ +10.35 after sell → book $10,087.90; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 274 | $5.07 | $3.59 | $-50.97 | $2,975.41 | ▼ -50.97 after sell → book $10,084.31; vs 09:30 mark -3.59 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $4,247.22 | ▼ -17.91 after sell → book $10,082.29; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 275 | $4.88 | $3.60 | $-100.65 | $5,585.62 | ▼ -100.65 after sell → book $10,078.69; vs 09:30 mark -3.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 96 | $14.54 | $2.31 | $-35.30 | $6,979.15 | ▼ -35.30 after sell → book $10,076.38; vs 09:30 mark -2.31 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 407 | $3.59 | $5.33 | $+13.84 | $8,434.95 | ▲ +13.84 after sell → book $10,071.05; vs 09:30 mark -5.33 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1222 | $1.38 | $15.76 | — | $6,732.83 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1686.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 105 | $15.94 | $2.31 | — | $5,056.83 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1686.99 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SIMO` | 6 | $272.00 | $2.01 | — | $3,422.82 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ⚪; ret5=-3.9; leftover $1686.99 | join🟢 sector🟢 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `EQ` | 714 | $2.36 | $9.21 | — | $1,728.57 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; ret5=-2.1; leftover $1686.99 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 57 | $29.33 | $2.16 | — | $54.60 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1686.99 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $54.60 | ▼ 09:30 equity $9,639.33 vs yday $9,917.69 (-278.36) | 09:30 open · cash $54.60 (unchanged overnight, no fees) · equity $9,639.33 vs prior close $9,917.69 (-278.36) because holdings re-marked: MOS×59 yday $23.76 → 09:30 $23.75 -0.59; RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×4 yday $14.62 → 09:30 $14.56 -0.24; DLO×4 yday $15.14 → 09:30 $15.01 -0.52; GEN×2 yday $30.50 → 09:30 $31.02 +1.04; LVWR×1222 yday $1.36 → 09:30 $1.37 +12.22; GRRR×105 yday $15.66 → 09:30 $14.32 -140.70; SIMO×6 yday $255.08 → 09:30 $246.79 -49.74; EQ×714 yday $2.45 → 09:30 $2.37 -57.12; ZYME×57 yday $29.01 → 09:30 $28.27 -42.18 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 59 | $23.75 | $2.19 | $-19.11 | $1,453.66 | ▼ -19.11 after sell → book $9,637.14; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,453.66 | ▼ 09:30 equity $9,521.15 vs yday $9,588.31 (-67.16) | 09:30 open · cash $1,453.66 (unchanged overnight, no fees) · equity $9,521.15 vs prior close $9,588.31 (-67.16) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×4 yday $14.51 → 09:30 $14.31 -0.80; DLO×4 yday $15.00 → 09:30 $14.88 -0.48; GEN×2 yday $31.02 → 09:30 $30.56 -0.92; LVWR×1222 yday $1.34 → 09:30 $1.22 -146.64; GRRR×105 yday $14.20 → 09:30 $15.05 +89.25; SIMO×6 yday $246.79 → 09:30 $247.53 +4.44; EQ×714 yday $2.37 → 09:30 $2.27 -71.40; ZYME×57 yday $28.27 → 09:30 $29.32 +59.85 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $1,494.54 | ▼ -0.25 after sell → book $9,520.71; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 4 | $14.31 | $0.60 | $-0.30 | $1,551.18 | ▼ -0.30 after sell → book $9,520.11; vs 09:30 mark -0.60 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 4 | $14.88 | $0.63 | $-4.14 | $1,610.07 | ▼ -4.14 after sell → book $9,519.48; vs 09:30 mark -0.63 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `GEN` | 2 | $30.56 | $0.64 | $+2.12 | $1,670.55 | ▲ +2.12 after sell → book $9,518.84; vs 09:30 mark -0.64 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,670.55 | ▼ 09:30 equity $9,391.76 vs yday $9,406.30 (-14.54) | 09:30 open · cash $1,670.55 (unchanged overnight, no fees) · equity $9,391.76 vs prior close $9,406.30 (-14.54) because holdings re-marked: LVWR×1222 yday $1.18 → 09:30 $1.19 +12.22; GRRR×105 yday $14.80 → 09:30 $14.75 -5.25; SIMO×6 yday $241.20 → 09:30 $240.09 -6.66; EQ×714 yday $2.27 → 09:30 $2.25 -14.28; ZYME×57 yday $29.33 → 09:30 $29.32 -0.57 | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1222 | $1.19 | $15.98 | $-263.92 | $3,108.75 | ▼ -263.92 after sell → book $9,375.78; vs 09:30 mark -15.98 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 105 | $14.75 | $2.33 | $-129.59 | $4,655.17 | ▼ -129.59 after sell → book $9,373.45; vs 09:30 mark -2.33 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SIMO` | 6 | $240.09 | $2.03 | $-195.50 | $6,093.68 | ▼ -195.50 after sell → book $9,371.42; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `EQ` | 714 | $2.25 | $9.34 | $-97.09 | $7,690.84 | ▼ -97.09 after sell → book $9,362.08; vs 09:30 mark -9.34 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 57 | $29.32 | $2.18 | $-4.92 | $9,359.89 | ▼ -4.92 after sell → book $9,359.89; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,359.89 | ▲ 09:30 equity $9,359.89 vs yday $9,359.89 (+0.00) | 09:30 open · cash $9,359.89 · no holdings · equity $9,359.89 vs prior close $9,359.89 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 23 | $49.76 | $2.06 | — | $8,213.35 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $7,077.88 | — | union ∩ candle, no 🚨; gate candle_capture=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 74 | $15.70 | $2.21 | — | $5,913.87 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1169.99 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $4,749.94 | — | union ∩ candle, no 🚨; gate candle_capture=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.46 | $2.20 | — | $3,579.08 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SID` | 1017 | $1.15 | $13.12 | — | $2,396.41 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+10.1; leftover $1169.99 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🔴 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 113 | $10.27 | $2.33 | — | $1,233.57 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 79 | $14.79 | $2.23 | — | $62.93 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer; 🔵; ret5=+5.8; leftover $1169.99 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.93 | ▲ 09:30 equity $9,685.36 vs yday $9,546.22 (+139.14) | 09:30 open · cash $62.93 (unchanged overnight, no fees) · equity $9,685.36 vs prior close $9,546.22 (+139.14) because holdings re-marked: ATRC×23 yday $52.59 → 09:30 $52.88 +6.67; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×74 yday $15.54 → 09:30 $15.45 -6.66; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; ARCT×71 yday $16.74 → 09:30 $16.77 +2.13; SID×1017 yday $1.17 → 09:30 $1.36 +193.23; NVAX×113 yday $10.32 → 09:30 $10.41 +10.17; CLYM×79 yday $15.05 → 09:30 $13.96 -86.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 2 | $5.08 | $0.11 | — | $52.67 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $10.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 3 | $2.70 | $0.09 | — | $44.48 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $10.49 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `UAMY` | 1 | $5.37 | $0.06 | — | $39.05 | — | union ∩ candle, no 🚨; gate candle_capture=True; list yday_gainer,yday_mover; ret5=-0.4; leftover $10.49 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟡 buy🟡 |

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
