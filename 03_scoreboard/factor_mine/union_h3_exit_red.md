# Factor mine action — `union_h3_exit_red`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · buy last-green, sell next 09:30 if last bar flipped red

Cash book **+4.14%** ($10,414) · signal-only (no cash/fees) was +31.33%. Starts YES **12/17**. Fills 80 · skips 149 · realized $+85.08.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $30.52.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, INO, TNDM | — | $56.25 | $10,286.85 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $56.25 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | $10,321.25 | +34.40 | LDI, BTBT, ANGX, HYLN | — | $34.95 | $10,677.53 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | 09:30 open · cash $56.25 (unchanged overnight, no fees) · equity $10,321.25 vs prior close $10,286.85 (+34.40) because holdings re-marked: BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; INO×2469 yday $0.90 → 09:30 $0.93 +74.07; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85 |
| 2026-08-17 | +2.25 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | $10,645.20 | -32.33 | — | — | $34.95 | $10,729.57 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,645.20 vs prior close $10,677.53 (-32.33) because holdings re-marked: BTSG×33 yday $61.71 → 09:30 $61.69 -0.66; IREN×43 yday $44.06 → 09:30 $45.23 +50.31; TPG×39 yday $53.03 → 09:30 $52.67 -14.04; INO×2469 yday $1.09 → 09:30 $1.07 -49.38; TNDM×85 yday $22.72 → 09:30 $22.50 -18.70; LDI×7 yday $0.90 → 09:30 $0.91 +0.07; BTBT×4 yday $1.57 → 09:30 $1.52 -0.20; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 |
| 2026-08-18 | -6.20 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | $10,626.31 | -103.26 | — | BTSG, IREN, TPG, INO, TNDM | $10,564.37 | $10,584.89 | LDI×7, BTBT×4, ANGX×1, HYLN×1 | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,626.31 vs prior close $10,729.57 (-103.26) because holdings re-marked: BTSG×33 yday $60.38 → 09:30 $60.00 -12.54; IREN×43 yday $44.90 → 09:30 $43.56 -57.62; TPG×39 yday $51.77 → 09:30 $51.77 +0.00; INO×2469 yday $1.15 → 09:30 $1.14 -24.69; TNDM×85 yday $22.25 → 09:30 $22.16 -8.07; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×4 yday $1.60 → 09:30 $1.54 -0.24; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14 |
| 2026-08-19 | -7.20 | $10,564.37 | LDI×7, BTBT×4, ANGX×1, HYLN×1 | $10,584.87 | -0.02 | — | LDI, BTBT, ANGX, HYLN | $10,584.55 | $10,584.55 | — | 09:30 open · cash $10,564.37 (unchanged overnight, no fees) · equity $10,584.87 vs prior close $10,584.89 (-0.02) because holdings re-marked: LDI×7 yday $0.86 → 09:30 $0.88 +0.15; BTBT×4 yday $1.45 → 09:30 $1.42 -0.12; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01 |
| 2026-08-20 | +1.12 | $10,584.55 | — | $10,584.55 | -0.00 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $38.21 | $10,730.97 | AG×64, CDE×64, HDSN×229, IAG×67, KGC×44, NFGC×756, WPM×9, ABUS×268 | 09:30 open · cash $10,584.55 · no holdings · equity $10,584.55 vs prior close $10,584.55 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $38.21 | AG×64, CDE×64, HDSN×229, IAG×67, KGC×44, NFGC×756, WPM×9, ABUS×268 | $11,103.25 | +372.28 | CYPH, BTBT | — | $30.84 | $11,084.43 | AG×64, CDE×64, HDSN×229, IAG×67, KGC×44, NFGC×756, WPM×9, ABUS×268, CYPH×3, BTBT×2 | 09:30 open · cash $38.21 (unchanged overnight, no fees) · equity $11,103.25 vs prior close $10,730.97 (+372.28) because holdings re-marked: AG×64 yday $21.19 → 09:30 $21.90 +45.44; CDE×64 yday $21.11 → 09:30 $21.75 +40.96; HDSN×229 yday $5.57 → 09:30 $5.67 +22.90; IAG×67 yday $20.50 → 09:30 $21.17 +44.89; KGC×44 yday $31.43 → 09:30 $32.17 +32.56; NFGC×756 yday $1.75 → 09:30 $1.79 +30.24; WPM×9 yday $150.25 → 09:30 $154.70 +40.05; ABUS×268 yday $4.77 → 09:30 $5.20 +115.24 |
| 2026-08-24 | -5.17 | $30.84 | AG×64, CDE×64, HDSN×229, IAG×67, KGC×44, NFGC×756, WPM×9, ABUS×268, CYPH×3, BTBT×2 | $11,199.92 | +115.49 | — | — | $30.84 | $11,054.05 | AG×64, CDE×64, HDSN×229, IAG×67, KGC×44, NFGC×756, WPM×9, ABUS×268, CYPH×3, BTBT×2 | 09:30 open · cash $30.84 (unchanged overnight, no fees) · equity $11,199.92 vs prior close $11,084.43 (+115.49) because holdings re-marked: AG×64 yday $21.09 → 09:30 $21.47 +24.32; CDE×64 yday $20.97 → 09:30 $21.26 +18.56; HDSN×229 yday $5.63 → 09:30 $5.69 +13.74; IAG×67 yday $21.14 → 09:30 $21.44 +20.10; KGC×44 yday $32.76 → 09:30 $33.21 +19.80; NFGC×756 yday $1.84 → 09:30 $1.86 +15.12; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; ABUS×268 yday $5.21 → 09:30 $5.18 -8.04; CYPH×3 yday $1.42 → 09:30 $1.83 +1.23; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04 |
| 2026-08-25 | +1.80 | $30.84 | AG×64, CDE×64, HDSN×229, IAG×67, KGC×44, NFGC×756, WPM×9, ABUS×268, CYPH×3, BTBT×2 | $11,150.82 | +96.77 | MOS, INSP, RZLT, HCA, NPWR, ALVO, ALIT, ZURA | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $151.36 | $11,144.57 | CYPH×3, BTBT×2, MOS×57, INSP×22, RZLT×265, HCA×3, NPWR×694, ALVO×266, ALIT×93, ZURA×217 | 09:30 open · cash $30.84 (unchanged overnight, no fees) · equity $11,150.82 vs prior close $11,054.05 (+96.77) because holdings re-marked: AG×64 yday $20.57 → 09:30 $20.73 +10.24; CDE×64 yday $20.49 → 09:30 $20.85 +23.04; HDSN×229 yday $5.57 → 09:30 $5.53 -9.16; IAG×67 yday $21.36 → 09:30 $21.63 +18.09; KGC×44 yday $32.47 → 09:30 $32.76 +12.76; NFGC×756 yday $1.90 → 09:30 $1.91 +7.56; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; ABUS×268 yday $5.20 → 09:30 $5.26 +16.08; CYPH×3 yday $1.64 → 09:30 $1.70 +0.18; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02 |
| 2026-08-26 | +2.02 | $151.36 | CYPH×3, BTBT×2, MOS×57, INSP×22, RZLT×265, HCA×3, NPWR×694, ALVO×266, ALIT×93, ZURA×217 | $11,144.57 | -0.00 | — | — | $151.36 | $11,096.53 | CYPH×3, BTBT×2, MOS×57, INSP×22, RZLT×265, HCA×3, NPWR×694, ALVO×266, ALIT×93, ZURA×217 | 09:30 open · cash $151.36 (unchanged overnight, no fees) · equity $11,144.57 vs prior close $11,144.57 (-0.00) because holdings re-marked: CYPH×3 yday $1.64 → 09:30 $1.64 +0.00; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; MOS×57 yday $23.75 → 09:30 $23.75 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; RZLT×265 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; NPWR×694 yday $2.02 → 09:30 $2.02 +0.00; ALVO×266 yday $5.25 → 09:30 $5.25 +0.00; ALIT×93 yday $14.87 → 09:30 $14.87 +0.00; ZURA×217 yday $6.50 → 09:30 $6.50 +0.00 |
| 2026-08-27 | — | $151.36 | CYPH×3, BTBT×2, MOS×57, INSP×22, RZLT×265, HCA×3, NPWR×694, ALVO×266, ALIT×93, ZURA×217 | $10,882.15 | -214.38 | CRK, SLI, DLO | CYPH, BTBT | $108.14 | $10,707.40 | MOS×57, INSP×22, RZLT×265, HCA×3, NPWR×694, ALVO×266, ALIT×93, ZURA×217, CRK×1, SLI×8, DLO×1 | 09:30 open · cash $151.36 (unchanged overnight, no fees) · equity $10,882.15 vs prior close $11,096.53 (-214.38) because holdings re-marked: CYPH×3 yday $1.64 → 09:30 $1.60 -0.12; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; MOS×57 yday $23.75 → 09:30 $24.84 +62.13; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; RZLT×265 yday $5.29 → 09:30 $5.01 -74.20; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; NPWR×694 yday $2.02 → 09:30 $1.93 -62.46; ALVO×266 yday $5.25 → 09:30 $4.98 -71.82; ALIT×93 yday $14.87 → 09:30 $14.85 -1.86; ZURA×217 yday $6.50 → 09:30 $6.13 -80.29 |
| 2026-08-28 | +0.75 | $108.14 | MOS×57, INSP×22, RZLT×265, HCA×3, NPWR×694, ALVO×266, ALIT×93, ZURA×217, CRK×1, SLI×8, DLO×1 | $10,736.93 | +29.53 | RRC, ANF, BHVN, BZ, LVWR | INSP, RZLT, HCA, NPWR, ALVO, ALIT, ZURA | $152.20 | $10,526.20 | MOS×57, CRK×1, SLI×8, DLO×1, RRC×44, ANF×12, BHVN×109, BZ×100, LVWR×1346 | 09:30 open · cash $108.14 (unchanged overnight, no fees) · equity $10,736.93 vs prior close $10,707.40 (+29.53) because holdings re-marked: MOS×57 yday $24.16 → 09:30 $24.00 -9.12; INSP×22 yday $61.80 → 09:30 $62.10 +6.60; RZLT×265 yday $5.04 → 09:30 $5.07 +7.95; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; NPWR×694 yday $1.81 → 09:30 $1.83 +13.88; ALVO×266 yday $4.91 → 09:30 $4.88 -7.98; ALIT×93 yday $14.33 → 09:30 $14.54 +19.53; ZURA×217 yday $5.99 → 09:30 $6.02 +6.51; CRK×1 yday $14.50 → 09:30 $14.42 -0.08; SLI×8 yday $2.61 → 09:30 $2.60 -0.08; DLO×1 yday $15.36 → 09:30 $15.33 -0.03 |
| 2026-08-31 | -5.85 | $152.20 | MOS×57, CRK×1, SLI×8, DLO×1, RRC×44, ANF×12, BHVN×109, BZ×100, LVWR×1346 | $10,464.46 | -61.74 | — | MOS | $1,503.77 | $10,455.28 | CRK×1, SLI×8, DLO×1, RRC×44, ANF×12, BHVN×109, BZ×100, LVWR×1346 | 09:30 open · cash $152.20 (unchanged overnight, no fees) · equity $10,464.46 vs prior close $10,526.20 (-61.74) because holdings re-marked: MOS×57 yday $23.76 → 09:30 $23.75 -0.57; CRK×1 yday $14.62 → 09:30 $14.56 -0.06; SLI×8 yday $2.64 → 09:30 $2.51 -1.04; DLO×1 yday $15.14 → 09:30 $15.01 -0.13; RRC×44 yday $41.64 → 09:30 $41.11 -23.32; ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BHVN×109 yday $16.12 → 09:30 $15.44 -74.12; BZ×100 yday $18.00 → 09:30 $17.89 -11.00; LVWR×1346 yday $1.36 → 09:30 $1.37 +13.46 |
| 2026-09-01 | -6.30 | $1,503.77 | CRK×1, SLI×8, DLO×1, RRC×44, ANF×12, BHVN×109, BZ×100, LVWR×1346 | $10,145.45 | -309.83 | — | CRK, SLI, DLO | $1,553.96 | $10,077.37 | RRC×44, ANF×12, BHVN×109, BZ×100, LVWR×1346 | 09:30 open · cash $1,503.77 (unchanged overnight, no fees) · equity $10,145.45 vs prior close $10,455.28 (-309.83) because holdings re-marked: CRK×1 yday $14.51 → 09:30 $14.31 -0.20; SLI×8 yday $2.51 → 09:30 $2.70 +1.52; DLO×1 yday $15.00 → 09:30 $14.88 -0.12; RRC×44 yday $41.78 → 09:30 $41.32 -20.24; ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BHVN×109 yday $15.40 → 09:30 $15.45 +5.45; BZ×100 yday $17.90 → 09:30 $17.37 -53.00; LVWR×1346 yday $1.34 → 09:30 $1.22 -161.52 |
| 2026-09-02 | -3.83 | $1,553.96 | RRC×44, ANF×12, BHVN×109, BZ×100, LVWR×1346 | $10,111.57 | +34.20 | — | RRC, ANF, BHVN, BZ, LVWR | $10,085.10 | $10,085.10 | — | 09:30 open · cash $1,553.96 (unchanged overnight, no fees) · equity $10,111.57 vs prior close $10,077.37 (+34.20) because holdings re-marked: RRC×44 yday $41.32 → 09:30 $41.94 +27.28; ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BHVN×109 yday $15.45 → 09:30 $15.39 -6.54; BZ×100 yday $17.17 → 09:30 $17.29 +12.00; LVWR×1346 yday $1.18 → 09:30 $1.19 +13.46 |
| 2026-09-03 | -0.90 | $10,085.10 | — | $10,085.10 | +0.00 | ATRC, HRMY, VSTM, RVTY, GPRO, CRK, MMED, SLN | — | $40.04 | $10,810.17 | ATRC×25, HRMY×30, VSTM×163, RVTY×10, GPRO×1033, CRK×80, MMED×55, SLN×85 | 09:30 open · cash $10,085.10 · no holdings · equity $10,085.10 vs prior close $10,085.10 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $40.04 | ATRC×25, HRMY×30, VSTM×163, RVTY×10, GPRO×1033, CRK×80, MMED×55, SLN×85 | $10,933.72 | +123.55 | BAK, EOSE | — | $30.52 | $10,414.40 | ATRC×25, HRMY×30, VSTM×163, RVTY×10, GPRO×1033, CRK×80, MMED×55, SLN×85, BAK×3, EOSE×1 | 09:30 open · cash $40.04 (unchanged overnight, no fees) · equity $10,933.72 vs prior close $10,810.17 (+123.55) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; VSTM×163 yday $8.02 → 09:30 $8.03 +1.63; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1033 yday $1.69 → 09:30 $1.78 +92.97; CRK×80 yday $15.54 → 09:30 $15.45 -7.20; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; SLN×85 yday $14.79 → 09:30 $14.85 +5.10 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.25 | ▲ 09:30 equity $10,321.25 vs yday $10,286.85 (+34.40) | 09:30 open · cash $56.25 (unchanged overnight, no fees) · equity $10,321.25 vs prior close $10,286.85 (+34.40) because holdings re-marked: BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; INO×2469 yday $0.90 → 09:30 $0.93 +74.07; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $49.60 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $43.53 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $39.18 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $34.95 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $10,645.20 vs yday $10,677.53 (-32.33) | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,645.20 vs prior close $10,677.53 (-32.33) because holdings re-marked: BTSG×33 yday $61.71 → 09:30 $61.69 -0.66; IREN×43 yday $44.06 → 09:30 $45.23 +50.31; TPG×39 yday $53.03 → 09:30 $52.67 -14.04; INO×2469 yday $1.09 → 09:30 $1.07 -49.38; TNDM×85 yday $22.72 → 09:30 $22.50 -18.70; LDI×7 yday $0.90 → 09:30 $0.91 +0.07; BTBT×4 yday $1.57 → 09:30 $1.52 -0.20; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $10,626.31 vs yday $10,729.57 (-103.26) | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,626.31 vs prior close $10,729.57 (-103.26) because holdings re-marked: BTSG×33 yday $60.38 → 09:30 $60.00 -12.54; IREN×43 yday $44.90 → 09:30 $43.56 -57.62; TPG×39 yday $51.77 → 09:30 $51.77 +0.00; INO×2469 yday $1.15 → 09:30 $1.14 -24.69; TNDM×85 yday $22.25 → 09:30 $22.16 -8.07; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×4 yday $1.60 → 09:30 $1.54 -0.24; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14 | — |
| 2026-08-18 09:30 ET | **SELL** | `BTSG` | 33 | $60.00 | $2.11 | $+2.40 | $2,012.84 | ▲ +2.40 after sell → book $10,624.20; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `IREN` | 43 | $43.56 | $2.14 | $-108.32 | $3,883.77 | ▼ -108.32 after sell → book $10,622.05; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TPG` | 39 | $51.77 | $2.13 | $+40.49 | $5,900.67 | ▲ +40.49 after sell → book $10,619.92; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `INO` | 2469 | $1.14 | $32.28 | $+755.08 | $8,683.05 | ▲ +755.08 after sell → book $10,587.64; vs 09:30 mark -32.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-18 09:30 ET | **SELL** | `TNDM` | 85 | $22.16 | $2.27 | $-103.97 | $10,564.37 | ▼ -103.97 after sell → book $10,585.36; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,564.37 | ▼ 09:30 equity $10,584.87 vs yday $10,584.89 (-0.02) | 09:30 open · cash $10,564.37 (unchanged overnight, no fees) · equity $10,584.87 vs prior close $10,584.89 (-0.02) because holdings re-marked: LDI×7 yday $0.86 → 09:30 $0.88 +0.15; BTBT×4 yday $1.45 → 09:30 $1.42 -0.12; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01 | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 7 | $0.88 | $0.10 | $-0.59 | $10,570.43 | ▼ -0.59 after sell → book $10,584.77; vs 09:30 mark -0.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 4 | $1.42 | $0.09 | $-0.48 | $10,576.02 | ▼ -0.48 after sell → book $10,584.68; vs 09:30 mark -0.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 1 | $4.79 | $0.07 | $+0.36 | $10,580.74 | ▲ +0.36 after sell → book $10,584.61; vs 09:30 mark -0.07 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 1 | $3.87 | $0.06 | $-0.42 | $10,584.55 | ▼ -0.42 after sell → book $10,584.55; vs 09:30 mark -0.06 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,584.55 | ▲ 09:30 equity $10,584.55 vs yday $10,584.55 (-0.00) | 09:30 open · cash $10,584.55 · no holdings · equity $10,584.55 vs prior close $10,584.55 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 64 | $20.55 | $2.18 | — | $9,267.17 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1323.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 64 | $20.65 | $2.18 | — | $7,943.38 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1323.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 229 | $5.77 | $2.95 | — | $6,619.10 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1323.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 67 | $19.63 | $2.19 | — | $5,301.70 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1323.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 44 | $29.63 | $2.12 | — | $3,995.86 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1323.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 756 | $1.75 | $9.75 | — | $2,663.10 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1323.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $1,360.23 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1323.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 268 | $4.92 | $3.46 | — | $38.21 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1323.07 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $38.21 | ▲ 09:30 equity $11,103.25 vs yday $10,730.97 (+372.28) | 09:30 open · cash $38.21 (unchanged overnight, no fees) · equity $11,103.25 vs prior close $10,730.97 (+372.28) because holdings re-marked: AG×64 yday $21.19 → 09:30 $21.90 +45.44; CDE×64 yday $21.11 → 09:30 $21.75 +40.96; HDSN×229 yday $5.57 → 09:30 $5.67 +22.90; IAG×67 yday $20.50 → 09:30 $21.17 +44.89; KGC×44 yday $31.43 → 09:30 $32.17 +32.56; NFGC×756 yday $1.75 → 09:30 $1.79 +30.24; WPM×9 yday $150.25 → 09:30 $154.70 +40.05; ABUS×268 yday $4.77 → 09:30 $5.20 +115.24 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 3 | $1.32 | $0.05 | — | $34.20 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $4.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $30.84 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.78 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.84 | ▲ 09:30 equity $11,199.92 vs yday $11,084.43 (+115.49) | 09:30 open · cash $30.84 (unchanged overnight, no fees) · equity $11,199.92 vs prior close $11,084.43 (+115.49) because holdings re-marked: AG×64 yday $21.09 → 09:30 $21.47 +24.32; CDE×64 yday $20.97 → 09:30 $21.26 +18.56; HDSN×229 yday $5.63 → 09:30 $5.69 +13.74; IAG×67 yday $21.14 → 09:30 $21.44 +20.10; KGC×44 yday $32.76 → 09:30 $33.21 +19.80; NFGC×756 yday $1.84 → 09:30 $1.86 +15.12; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; ABUS×268 yday $5.21 → 09:30 $5.18 -8.04; CYPH×3 yday $1.42 → 09:30 $1.83 +1.23; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $30.84 | ▲ 09:30 equity $11,150.82 vs yday $11,054.05 (+96.77) | 09:30 open · cash $30.84 (unchanged overnight, no fees) · equity $11,150.82 vs prior close $11,054.05 (+96.77) because holdings re-marked: AG×64 yday $20.57 → 09:30 $20.73 +10.24; CDE×64 yday $20.49 → 09:30 $20.85 +23.04; HDSN×229 yday $5.57 → 09:30 $5.53 -9.16; IAG×67 yday $21.36 → 09:30 $21.63 +18.09; KGC×44 yday $32.47 → 09:30 $32.76 +12.76; NFGC×756 yday $1.90 → 09:30 $1.91 +7.56; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; ABUS×268 yday $5.20 → 09:30 $5.26 +16.08; CYPH×3 yday $1.64 → 09:30 $1.70 +0.18; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 64 | $20.73 | $2.20 | $+7.13 | $1,355.36 | ▲ +7.13 after sell → book $11,148.62; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 64 | $20.85 | $2.20 | $+8.41 | $2,687.56 | ▲ +8.41 after sell → book $11,146.42; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 229 | $5.53 | $3.00 | $-60.92 | $3,950.92 | ▼ -60.92 after sell → book $11,143.41; vs 09:30 mark -3.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 67 | $21.63 | $2.21 | $+129.60 | $5,397.92 | ▲ +129.60 after sell → book $11,141.20; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 44 | $32.76 | $2.14 | $+133.45 | $6,837.22 | ▲ +133.45 after sell → book $11,139.06; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 756 | $1.91 | $9.89 | $+101.32 | $8,271.29 | ▲ +101.32 after sell → book $11,129.17; vs 09:30 mark -9.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 9 | $160.00 | $2.04 | $+135.08 | $9,709.25 | ▲ +135.08 after sell → book $11,127.13; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 268 | $5.26 | $3.51 | $+84.15 | $11,115.42 | ▲ +84.15 after sell → book $11,123.62; vs 09:30 mark -3.51 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 57 | $24.00 | $2.16 | — | $9,745.25 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ⚪; ret5=+13.0; leftover $1389.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 22 | $61.47 | $2.06 | — | $8,390.86 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ret5=+9.2; leftover $1389.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 265 | $5.23 | $3.42 | — | $7,001.49 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+10.7; leftover $1389.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 3 | $429.24 | $2.00 | — | $5,711.77 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+6.1; leftover $1389.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 694 | $2.00 | $8.95 | — | $4,314.82 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1389.43 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 266 | $5.22 | $3.43 | — | $2,922.87 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1389.43 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 93 | $14.86 | $2.27 | — | $1,538.62 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1389.43 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 217 | $6.38 | $2.80 | — | $151.36 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1389.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.36 | ▲ 09:30 equity $11,144.57 vs yday $11,144.57 (-0.00) | 09:30 open · cash $151.36 (unchanged overnight, no fees) · equity $11,144.57 vs prior close $11,144.57 (-0.00) because holdings re-marked: CYPH×3 yday $1.64 → 09:30 $1.64 +0.00; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; MOS×57 yday $23.75 → 09:30 $23.75 +0.00; INSP×22 yday $61.47 → 09:30 $61.47 +0.00; RZLT×265 yday $5.29 → 09:30 $5.29 +0.00; HCA×3 yday $428.50 → 09:30 $428.50 +0.00; NPWR×694 yday $2.02 → 09:30 $2.02 +0.00; ALVO×266 yday $5.25 → 09:30 $5.25 +0.00; ALIT×93 yday $14.87 → 09:30 $14.87 +0.00; ZURA×217 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.36 | ▼ 09:30 equity $10,882.15 vs yday $11,096.53 (-214.38) | 09:30 open · cash $151.36 (unchanged overnight, no fees) · equity $10,882.15 vs prior close $11,096.53 (-214.38) because holdings re-marked: CYPH×3 yday $1.64 → 09:30 $1.60 -0.12; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; MOS×57 yday $23.75 → 09:30 $24.84 +62.13; INSP×22 yday $61.47 → 09:30 $60.07 -30.80; RZLT×265 yday $5.29 → 09:30 $5.01 -74.20; HCA×3 yday $428.50 → 09:30 $427.50 -3.00; NPWR×694 yday $2.02 → 09:30 $1.93 -62.46; ALVO×266 yday $5.25 → 09:30 $4.98 -71.82; ALIT×93 yday $14.87 → 09:30 $14.85 -1.86; ZURA×217 yday $6.50 → 09:30 $6.13 -80.29 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 3 | $1.60 | $0.08 | $+0.71 | $156.08 | ▲ +0.71 after sell → book $10,882.07; vs 09:30 mark -0.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $159.09 | ▼ -0.36 after sell → book $10,882.02; vs 09:30 mark -0.05 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 1 | $14.09 | $0.14 | — | $144.85 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+1.1; leftover $22.73 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 8 | $2.59 | $0.23 | — | $123.90 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+4.2; leftover $22.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 1 | $15.60 | $0.16 | — | $108.14 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list mover_buy; 🔵; ret5=+7.1; leftover $22.73 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $108.14 | ▲ 09:30 equity $10,736.93 vs yday $10,707.40 (+29.53) | 09:30 open · cash $108.14 (unchanged overnight, no fees) · equity $10,736.93 vs prior close $10,707.40 (+29.53) because holdings re-marked: MOS×57 yday $24.16 → 09:30 $24.00 -9.12; INSP×22 yday $61.80 → 09:30 $62.10 +6.60; RZLT×265 yday $5.04 → 09:30 $5.07 +7.95; HCA×3 yday $427.16 → 09:30 $424.61 -7.65; NPWR×694 yday $1.81 → 09:30 $1.83 +13.88; ALVO×266 yday $4.91 → 09:30 $4.88 -7.98; ALIT×93 yday $14.33 → 09:30 $14.54 +19.53; ZURA×217 yday $5.99 → 09:30 $6.02 +6.51; CRK×1 yday $14.50 → 09:30 $14.42 -0.08; SLI×8 yday $2.61 → 09:30 $2.60 -0.08; DLO×1 yday $15.36 → 09:30 $15.33 -0.03 | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 22 | $62.10 | $2.08 | $+9.73 | $1,472.26 | ▲ +9.73 after sell → book $10,734.85; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 265 | $5.07 | $3.47 | $-49.29 | $2,812.34 | ▼ -49.29 after sell → book $10,731.38; vs 09:30 mark -3.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 3 | $424.61 | $2.02 | $-17.91 | $4,084.15 | ▼ -17.91 after sell → book $10,729.36; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 694 | $1.83 | $9.08 | $-136.01 | $5,345.09 | ▼ -136.01 after sell → book $10,720.28; vs 09:30 mark -9.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 266 | $4.88 | $3.49 | $-97.36 | $6,639.69 | ▼ -97.36 after sell → book $10,716.80; vs 09:30 mark -3.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 93 | $14.54 | $2.30 | $-34.32 | $7,989.61 | ▼ -34.32 after sell → book $10,714.50; vs 09:30 mark -2.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 217 | $6.02 | $2.85 | $-83.77 | $9,293.11 | ▼ -83.77 after sell → book $10,711.66; vs 09:30 mark -2.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `RRC` | 44 | $41.44 | $2.12 | — | $7,467.63 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; ret5=+1.8; leftover $1858.62 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 12 | $144.70 | $2.03 | — | $5,729.20 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1858.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 109 | $16.95 | $2.32 | — | $3,879.33 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1858.62 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 100 | $18.50 | $2.29 | — | $2,027.04 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1858.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 1346 | $1.38 | $17.36 | — | $152.20 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1858.62 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.20 | ▼ 09:30 equity $10,464.46 vs yday $10,526.20 (-61.74) | 09:30 open · cash $152.20 (unchanged overnight, no fees) · equity $10,464.46 vs prior close $10,526.20 (-61.74) because holdings re-marked: MOS×57 yday $23.76 → 09:30 $23.75 -0.57; CRK×1 yday $14.62 → 09:30 $14.56 -0.06; SLI×8 yday $2.64 → 09:30 $2.51 -1.04; DLO×1 yday $15.14 → 09:30 $15.01 -0.13; RRC×44 yday $41.64 → 09:30 $41.11 -23.32; ANF×12 yday $145.75 → 09:30 $148.67 +35.04; BHVN×109 yday $16.12 → 09:30 $15.44 -74.12; BZ×100 yday $18.00 → 09:30 $17.89 -11.00; LVWR×1346 yday $1.36 → 09:30 $1.37 +13.46 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 57 | $23.75 | $2.18 | $-18.59 | $1,503.77 | ▼ -18.59 after sell → book $10,462.28; vs 09:30 mark -2.18 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,503.77 | ▼ 09:30 equity $10,145.45 vs yday $10,455.28 (-309.83) | 09:30 open · cash $1,503.77 (unchanged overnight, no fees) · equity $10,145.45 vs prior close $10,455.28 (-309.83) because holdings re-marked: CRK×1 yday $14.51 → 09:30 $14.31 -0.20; SLI×8 yday $2.51 → 09:30 $2.70 +1.52; DLO×1 yday $15.00 → 09:30 $14.88 -0.12; RRC×44 yday $41.78 → 09:30 $41.32 -20.24; ANF×12 yday $149.28 → 09:30 $142.47 -81.72; BHVN×109 yday $15.40 → 09:30 $15.45 +5.45; BZ×100 yday $17.90 → 09:30 $17.37 -53.00; LVWR×1346 yday $1.34 → 09:30 $1.22 -161.52 | — |
| 2026-09-01 09:30 ET | **SELL** | `CRK` | 1 | $14.31 | $0.17 | $-0.09 | $1,517.91 | ▼ -0.09 after sell → book $10,145.28; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `SLI` | 8 | $2.70 | $0.26 | $+0.39 | $1,539.25 | ▲ +0.39 after sell → book $10,145.02; vs 09:30 mark -0.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-01 09:30 ET | **SELL** | `DLO` | 1 | $14.88 | $0.17 | $-1.05 | $1,553.96 | ▼ -1.05 after sell → book $10,144.85; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,553.96 | ▲ 09:30 equity $10,111.57 vs yday $10,077.37 (+34.20) | 09:30 open · cash $1,553.96 (unchanged overnight, no fees) · equity $10,111.57 vs prior close $10,077.37 (+34.20) because holdings re-marked: RRC×44 yday $41.32 → 09:30 $41.94 +27.28; ANF×12 yday $143.00 → 09:30 $142.00 -12.00; BHVN×109 yday $15.45 → 09:30 $15.39 -6.54; BZ×100 yday $17.17 → 09:30 $17.29 +12.00; LVWR×1346 yday $1.18 → 09:30 $1.19 +13.46 | — |
| 2026-09-02 09:30 ET | **SELL** | `RRC` | 44 | $41.94 | $2.15 | $+17.73 | $3,397.17 | ▲ +17.73 after sell → book $10,109.42; vs 09:30 mark -2.15 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 12 | $142.00 | $2.05 | $-36.48 | $5,099.12 | ▼ -36.48 after sell → book $10,107.37; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 109 | $15.39 | $2.35 | $-174.71 | $6,774.28 | ▼ -174.71 after sell → book $10,105.02; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 100 | $17.29 | $2.32 | $-125.61 | $8,500.96 | ▼ -125.61 after sell → book $10,102.70; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 1346 | $1.19 | $17.60 | $-290.70 | $10,085.10 | ▼ -290.70 after sell → book $10,085.10; vs 09:30 mark -17.60 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,085.10 | ▲ 09:30 equity $10,085.10 vs yday $10,085.10 (+0.00) | 09:30 open · cash $10,085.10 · no holdings · equity $10,085.10 vs prior close $10,085.10 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $49.76 | $2.06 | — | $8,839.04 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1260.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $7,597.66 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1260.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 163 | $7.70 | $2.48 | — | $6,340.08 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1260.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $5,078.66 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1260.64 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1033 | $1.22 | $13.33 | — | $3,805.07 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1260.64 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 80 | $15.70 | $2.23 | — | $2,546.84 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1260.64 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 55 | $22.78 | $2.15 | — | $1,291.79 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1260.64 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 85 | $14.70 | $2.25 | — | $40.04 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1260.64 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $40.04 | ▲ 09:30 equity $10,933.72 vs yday $10,810.17 (+123.55) | 09:30 open · cash $40.04 (unchanged overnight, no fees) · equity $10,933.72 vs prior close $10,810.17 (+123.55) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; VSTM×163 yday $8.02 → 09:30 $8.03 +1.63; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; GPRO×1033 yday $1.69 → 09:30 $1.78 +92.97; CRK×80 yday $15.54 → 09:30 $15.45 -7.20; MMED×55 yday $23.76 → 09:30 $23.88 +6.60; SLN×85 yday $14.79 → 09:30 $14.85 +5.10 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 3 | $1.95 | $0.07 | — | $34.13 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $6.67 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 1 | $3.57 | $0.04 | — | $30.52 | — | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $6.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `BTSG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `IREN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TPG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-14 | `VST` | cash | leftover split 7.03 < 1 share @ 146.90 |
| 2026-08-14 | `DAVE` | cash | leftover split 7.03 < 1 share @ 330.91 |
| 2026-08-14 | `SLG` | cash | leftover split 7.03 < 1 share @ 57.61 |
| 2026-08-14 | `BETR` | cash | leftover split 7.03 < 1 share @ 14.80 |
| 2026-08-17 | `BTSG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `IREN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TPG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DVN` | cash | leftover split 4.37 < 1 share @ 46.18 |
| 2026-08-17 | `EOG` | cash | leftover split 4.37 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 4.37 < 1 share @ 202.70 |
| 2026-08-17 | `NB` | cash | leftover split 4.37 < 1 share @ 5.07 |
| 2026-08-17 | `CDNL` | cash | leftover split 4.37 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 4.37 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 4.37 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 4.37 < 1 share @ 92.99 |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 4.78 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 4.78 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 4.78 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 4.78 < 1 share @ 11.13 |
| 2026-08-21 | `DE` | cash | leftover split 4.78 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 4.78 < 1 share @ 14.96 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RRC` | cash | leftover split 22.73 < 1 share @ 40.72 |
| 2026-08-27 | `ANET` | cash | leftover split 22.73 < 1 share @ 190.90 |
| 2026-08-27 | `ASML` | cash | leftover split 22.73 < 1 share @ 1746.33 |
| 2026-08-27 | `GEN` | cash | leftover split 22.73 < 1 share @ 28.89 |
| 2026-08-28 | `DLO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `DLO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PANW` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
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
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSCR` | cash | leftover split 6.67 < 1 share @ 30.65 |
| 2026-09-04 | `NVAX` | cash | leftover split 6.67 < 1 share @ 10.41 |
| 2026-09-04 | `BVS` | cash | leftover split 6.67 < 1 share @ 14.50 |
| 2026-09-04 | `DELL` | cash | leftover split 6.67 < 1 share @ 486.31 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 25 | 2026-09-03 @ $49.76 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1260.64 |
| `HRMY` | 30 | 2026-09-03 @ $41.31 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1260.64 |
| `VSTM` | 163 | 2026-09-03 @ $7.70 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1260.64 |
| `RVTY` | 10 | 2026-09-03 @ $125.94 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1260.64 |
| `GPRO` | 1033 | 2026-09-03 @ $1.22 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1260.64 |
| `CRK` | 80 | 2026-09-03 @ $15.70 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1260.64 |
| `MMED` | 55 | 2026-09-03 @ $22.78 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1260.64 |
| `SLN` | 85 | 2026-09-03 @ $14.70 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1260.64 |
| `BAK` | 3 | 2026-09-04 @ $1.95 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $6.67 |
| `EOSE` | 1 | 2026-09-04 @ $3.57 | buy last-green, sell next 09:30 if last bar flipped red; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $6.67 |
