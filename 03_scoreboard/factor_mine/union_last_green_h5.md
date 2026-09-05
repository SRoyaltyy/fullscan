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

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | BTSG, IREN, TPG, INO, TNDM | — | $56.25 | $10,286.85 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $56.25 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85 | $10,321.25 | +34.40 | LDI, BTBT, ANGX, HYLN | — | $34.95 | $10,677.53 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | 09:30 open · cash $56.25 (unchanged overnight, no fees) · equity $10,321.25 vs prior close $10,286.85 (+34.40) because holdings re-marked: BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; INO×2469 yday $0.90 → 09:30 $0.93 +74.07; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85 |
| 2026-08-17 | +2.25 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | $10,645.20 | -32.33 | — | — | $34.95 | $10,729.57 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,645.20 vs prior close $10,677.53 (-32.33) because holdings re-marked: BTSG×33 yday $61.71 → 09:30 $61.69 -0.66; IREN×43 yday $44.06 → 09:30 $45.23 +50.31; TPG×39 yday $53.03 → 09:30 $52.67 -14.04; INO×2469 yday $1.09 → 09:30 $1.07 -49.38; TNDM×85 yday $22.72 → 09:30 $22.50 -18.70; LDI×7 yday $0.90 → 09:30 $0.91 +0.07; BTBT×4 yday $1.57 → 09:30 $1.52 -0.20; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 |
| 2026-08-18 | -6.20 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | $10,626.31 | -103.26 | — | — | $34.95 | $10,833.60 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,626.31 vs prior close $10,729.57 (-103.26) because holdings re-marked: BTSG×33 yday $60.38 → 09:30 $60.00 -12.54; IREN×43 yday $44.90 → 09:30 $43.56 -57.62; TPG×39 yday $51.77 → 09:30 $51.77 +0.00; INO×2469 yday $1.15 → 09:30 $1.14 -24.69; TNDM×85 yday $22.25 → 09:30 $22.16 -8.07; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×4 yday $1.60 → 09:30 $1.54 -0.24; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14 |
| 2026-08-19 | -7.20 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | $10,928.57 | +94.97 | — | — | $34.95 | $11,132.78 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,928.57 vs prior close $10,833.60 (+94.97) because holdings re-marked: BTSG×33 yday $59.50 → 09:30 $60.15 +21.45; IREN×43 yday $42.00 → 09:30 $41.41 -25.16; TPG×39 yday $52.02 → 09:30 $52.26 +9.36; INO×2469 yday $1.20 → 09:30 $1.22 +49.38; TNDM×85 yday $23.73 → 09:30 $24.20 +39.95; LDI×7 yday $0.86 → 09:30 $0.88 +0.15; BTBT×4 yday $1.45 → 09:30 $1.42 -0.12; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01 |
| 2026-08-20 | +1.12 | $34.95 | BTSG×33, IREN×43, TPG×39, INO×2469, TNDM×85, LDI×7, BTBT×4, ANGX×1, HYLN×1 | $11,059.34 | -73.44 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | BTSG, IREN, TPG, INO, TNDM | $93.94 | $11,169.22 | LDI×7, BTBT×4, ANGX×1, HYLN×1, AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279 | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $11,059.34 vs prior close $11,132.78 (-73.44) because holdings re-marked: BTSG×33 yday $59.33 → 09:30 $58.64 -22.77; IREN×43 yday $42.84 → 09:30 $42.46 -16.34; TPG×39 yday $53.18 → 09:30 $53.06 -4.68; INO×2469 yday $1.30 → 09:30 $1.30 +0.00; TNDM×85 yday $23.46 → 09:30 $23.11 -29.75; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×4 yday $1.40 → 09:30 $1.46 +0.22; ANGX×1 yday $4.60 → 09:30 $4.57 -0.03; HYLN×1 yday $3.67 → 09:30 $3.61 -0.06 |
| 2026-08-21 | +3.25 | $93.94 | LDI×7, BTBT×4, ANGX×1, HYLN×1, AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279 | $11,554.83 | +385.61 | ARCT, CYPH, QDEL | LDI, ANGX, HYLN | $66.58 | $11,537.18 | BTBT×4, AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1 | 09:30 open · cash $93.94 (unchanged overnight, no fees) · equity $11,554.83 vs prior close $11,169.22 (+385.61) because holdings re-marked: LDI×7 yday $0.87 → 09:30 $0.87 -0.02; BTBT×4 yday $1.59 → 09:30 $1.66 +0.26; ANGX×1 yday $4.37 → 09:30 $4.43 +0.06; HYLN×1 yday $3.37 → 09:30 $3.42 +0.05; AG×66 yday $21.19 → 09:30 $21.90 +46.86; CDE×66 yday $21.11 → 09:30 $21.75 +42.24; HDSN×238 yday $5.57 → 09:30 $5.67 +23.80; IAG×70 yday $20.50 → 09:30 $21.17 +46.90; KGC×46 yday $31.43 → 09:30 $32.17 +34.04; NFGC×785 yday $1.75 → 09:30 $1.79 +31.40; WPM×9 yday $150.25 → 09:30 $154.70 +40.05; ABUS×279 yday $4.77 → 09:30 $5.20 +119.97 |
| 2026-08-24 | -5.17 | $66.58 | BTBT×4, AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1 | $11,659.70 | +122.52 | — | BTBT | $72.69 | $11,507.59 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1 | 09:30 open · cash $66.58 (unchanged overnight, no fees) · equity $11,659.70 vs prior close $11,537.18 (+122.52) because holdings re-marked: BTBT×4 yday $1.53 → 09:30 $1.55 +0.08; AG×66 yday $21.09 → 09:30 $21.47 +25.08; CDE×66 yday $20.97 → 09:30 $21.26 +19.14; HDSN×238 yday $5.63 → 09:30 $5.69 +14.28; IAG×70 yday $21.14 → 09:30 $21.44 +21.00; KGC×46 yday $32.76 → 09:30 $33.21 +20.70; NFGC×785 yday $1.84 → 09:30 $1.86 +15.70; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; ABUS×279 yday $5.21 → 09:30 $5.18 -8.37; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; CYPH×11 yday $1.42 → 09:30 $1.83 +4.51; QDEL×1 yday $14.74 → 09:30 $14.71 -0.03 |
| 2026-08-25 | +1.80 | $72.69 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1 | $11,608.59 | +101.00 | RZLT, NPWR, ALVO, ZURA | — | $47.59 | $11,525.26 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1 | 09:30 open · cash $72.69 (unchanged overnight, no fees) · equity $11,608.59 vs prior close $11,507.59 (+101.00) because holdings re-marked: AG×66 yday $20.57 → 09:30 $20.73 +10.56; CDE×66 yday $20.49 → 09:30 $20.85 +23.76; HDSN×238 yday $5.57 → 09:30 $5.53 -9.52; IAG×70 yday $21.36 → 09:30 $21.63 +18.90; KGC×46 yday $32.47 → 09:30 $32.76 +13.34; NFGC×785 yday $1.90 → 09:30 $1.91 +7.85; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; ABUS×279 yday $5.20 → 09:30 $5.26 +16.74; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; CYPH×11 yday $1.64 → 09:30 $1.70 +0.66; QDEL×1 yday $14.36 → 09:30 $14.49 +0.13 |
| 2026-08-26 | +2.02 | $47.59 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1 | $11,525.26 | -0.00 | — | — | $47.59 | $11,608.32 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1 | 09:30 open · cash $47.59 (unchanged overnight, no fees) · equity $11,525.26 vs prior close $11,525.26 (-0.00) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.68 +0.00; CDE×66 yday $20.71 → 09:30 $20.71 +0.00; HDSN×238 yday $5.49 → 09:30 $5.49 +0.00; IAG×70 yday $21.48 → 09:30 $21.48 +0.00; KGC×46 yday $32.55 → 09:30 $32.55 +0.00; NFGC×785 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; ABUS×279 yday $5.20 → 09:30 $5.20 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; CYPH×11 yday $1.64 → 09:30 $1.64 +0.00; QDEL×1 yday $14.49 → 09:30 $14.49 +0.00; RZLT×1 yday $5.29 → 09:30 $5.29 +0.00; NPWR×4 yday $2.02 → 09:30 $2.02 +0.00; ALVO×1 yday $5.25 → 09:30 $5.25 +0.00; ZURA×1 yday $6.50 → 09:30 $6.50 +0.00 |
| 2026-08-27 | — | $47.59 | AG×66, CDE×66, HDSN×238, IAG×70, KGC×46, NFGC×785, WPM×9, ABUS×279, ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1 | $11,673.01 | +64.69 | RRC, CRK, MOS, SLI, ANET, DLO, GEN | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $1,588.76 | $11,762.39 | ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50 | 09:30 open · cash $47.59 (unchanged overnight, no fees) · equity $11,673.01 vs prior close $11,608.32 (+64.69) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.63 -3.30; CDE×66 yday $20.71 → 09:30 $21.00 +19.14; HDSN×238 yday $5.49 → 09:30 $5.51 +4.76; IAG×70 yday $21.48 → 09:30 $21.64 +11.20; KGC×46 yday $32.55 → 09:30 $32.90 +16.10; NFGC×785 yday $1.90 → 09:30 $2.00 +78.50; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; ABUS×279 yday $5.20 → 09:30 $5.19 -2.79; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; CYPH×11 yday $1.64 → 09:30 $1.60 -0.44; QDEL×1 yday $14.49 → 09:30 $15.09 +0.60; RZLT×1 yday $5.29 → 09:30 $5.01 -0.28; NPWR×4 yday $2.02 → 09:30 $1.93 -0.36; ALVO×1 yday $5.25 → 09:30 $4.98 -0.27; ZURA×1 yday $6.50 → 09:30 $6.13 -0.37 |
| 2026-08-28 | +0.75 | $1,588.76 | ARCT×1, CYPH×11, QDEL×1, RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50 | $11,769.16 | +6.77 | ANF, BHVN, BZ, LVWR | ARCT, CYPH, QDEL | $116.46 | $11,742.02 | RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | 09:30 open · cash $1,588.76 (unchanged overnight, no fees) · equity $11,769.16 vs prior close $11,762.39 (+6.77) because holdings re-marked: ARCT×1 yday $15.83 → 09:30 $15.74 -0.09; CYPH×11 yday $1.63 → 09:30 $1.75 +1.32; QDEL×1 yday $14.91 → 09:30 $14.92 +0.01; RZLT×1 yday $5.04 → 09:30 $5.07 +0.03; NPWR×4 yday $1.81 → 09:30 $1.83 +0.08; ALVO×1 yday $4.91 → 09:30 $4.88 -0.03; ZURA×1 yday $5.99 → 09:30 $6.02 +0.03; RRC×35 yday $41.55 → 09:30 $41.44 -3.85; CRK×102 yday $14.50 → 09:30 $14.42 -8.16; MOS×58 yday $24.16 → 09:30 $24.00 -9.28; SLI×558 yday $2.61 → 09:30 $2.60 -5.58; ANET×7 yday $202.25 → 09:30 $205.90 +25.55; DLO×92 yday $15.36 → 09:30 $15.33 -2.76; GEN×50 yday $29.64 → 09:30 $29.83 +9.50 |
| 2026-08-31 | -5.85 | $116.46 | RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | $11,632.86 | -109.16 | — | — | $116.46 | $11,621.93 | RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | 09:30 open · cash $116.46 (unchanged overnight, no fees) · equity $11,632.86 vs prior close $11,742.02 (-109.16) because holdings re-marked: RZLT×1 yday $4.98 → 09:30 $4.62 -0.36; NPWR×4 yday $1.89 → 09:30 $1.83 -0.24; ALVO×1 yday $4.88 → 09:30 $4.98 +0.10; ZURA×1 yday $5.85 → 09:30 $5.51 -0.34; RRC×35 yday $41.64 → 09:30 $41.11 -18.55; CRK×102 yday $14.62 → 09:30 $14.56 -6.12; MOS×58 yday $23.76 → 09:30 $23.75 -0.58; SLI×558 yday $2.64 → 09:30 $2.51 -72.54; ANET×7 yday $201.09 → 09:30 $199.00 -14.63; DLO×92 yday $15.14 → 09:30 $15.01 -11.96; GEN×50 yday $30.50 → 09:30 $31.02 +26.00; ANF×2 yday $145.75 → 09:30 $148.67 +5.84; BHVN×24 yday $16.12 → 09:30 $15.44 -16.32; BZ×22 yday $18.00 → 09:30 $17.89 -2.42; LVWR×296 yday $1.36 → 09:30 $1.37 +2.96 |
| 2026-09-01 | -6.30 | $116.46 | RZLT×1, NPWR×4, ALVO×1, ZURA×1, RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | $11,615.69 | -6.24 | — | RZLT, NPWR, ALVO, ZURA | $138.78 | $11,635.20 | RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | 09:30 open · cash $116.46 (unchanged overnight, no fees) · equity $11,615.69 vs prior close $11,621.93 (-6.24) because holdings re-marked: RZLT×1 yday $4.62 → 09:30 $4.69 +0.07; NPWR×4 yday $1.82 → 09:30 $1.78 -0.16; ALVO×1 yday $4.96 → 09:30 $5.24 +0.28; ZURA×1 yday $5.64 → 09:30 $5.60 -0.04; RRC×35 yday $41.78 → 09:30 $41.32 -16.10; CRK×102 yday $14.51 → 09:30 $14.31 -20.40; MOS×58 yday $23.78 → 09:30 $24.00 +12.76; SLI×558 yday $2.51 → 09:30 $2.70 +106.02; ANET×7 yday $195.89 → 09:30 $196.60 +4.97; DLO×92 yday $15.00 → 09:30 $14.88 -11.04; GEN×50 yday $31.02 → 09:30 $30.56 -23.00; ANF×2 yday $149.28 → 09:30 $142.47 -13.62; BHVN×24 yday $15.40 → 09:30 $15.45 +1.20; BZ×22 yday $17.90 → 09:30 $17.37 -11.66; LVWR×296 yday $1.34 → 09:30 $1.22 -35.52 |
| 2026-09-02 | -3.83 | $138.78 | RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | $11,735.69 | +100.49 | — | — | $138.78 | $11,656.32 | RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | 09:30 open · cash $138.78 (unchanged overnight, no fees) · equity $11,735.69 vs prior close $11,635.20 (+100.49) because holdings re-marked: RRC×35 yday $41.32 → 09:30 $41.94 +21.70; CRK×102 yday $14.90 → 09:30 $15.82 +93.84; MOS×58 yday $24.25 → 09:30 $23.94 -17.98; SLI×558 yday $2.70 → 09:30 $2.67 -16.74; ANET×7 yday $193.30 → 09:30 $195.77 +17.29; DLO×92 yday $14.70 → 09:30 $14.61 -8.28; GEN×50 yday $30.56 → 09:30 $30.73 +8.50; ANF×2 yday $143.00 → 09:30 $142.00 -2.00; BHVN×24 yday $15.45 → 09:30 $15.39 -1.44; BZ×22 yday $17.17 → 09:30 $17.29 +2.64; LVWR×296 yday $1.18 → 09:30 $1.19 +2.96 |
| 2026-09-03 | -0.90 | $138.78 | RRC×35, CRK×102, MOS×58, SLI×558, ANET×7, DLO×92, GEN×50, ANF×2, BHVN×24, BZ×22, LVWR×296 | $11,614.34 | -41.98 | ATRC, HRMY, VSTM, RVTY, GPRO, MMED, SLN | RRC, MOS, SLI, ANET, DLO, GEN | $166.91 | $12,283.28 | CRK×102, ANF×2, BHVN×24, BZ×22, LVWR×296, ATRC×24, HRMY×29, VSTM×159, RVTY×9, GPRO×1006, MMED×53, SLN×83 | 09:30 open · cash $138.78 (unchanged overnight, no fees) · equity $11,614.34 vs prior close $11,656.32 (-41.98) because holdings re-marked: RRC×35 yday $42.40 → 09:30 $42.10 -10.50; CRK×102 yday $16.02 → 09:30 $15.70 -32.64; MOS×58 yday $24.78 → 09:30 $24.70 -4.64; SLI×558 yday $2.49 → 09:30 $2.49 +0.00; ANET×7 yday $189.26 → 09:30 $188.00 -8.82; DLO×92 yday $14.83 → 09:30 $14.82 -0.92; GEN×50 yday $30.02 → 09:30 $30.04 +1.00; ANF×2 yday $140.68 → 09:30 $139.65 -2.06; BHVN×24 yday $15.74 → 09:30 $15.97 +5.52; BZ×22 yday $17.55 → 09:30 $17.65 +2.20; LVWR×296 yday $1.14 → 09:30 $1.17 +8.88 |
| 2026-09-04 | — | $166.91 | CRK×102, ANF×2, BHVN×24, BZ×22, LVWR×296, ATRC×24, HRMY×29, VSTM×159, RVTY×9, GPRO×1006, MMED×53, SLN×83 | $12,398.49 | +115.21 | OSCR, NVAX, BVS, BAK, EOSE, DELL | CRK, ANF, BHVN, BZ, LVWR | $69.56 | $11,941.49 | ATRC×24, HRMY×29, VSTM×159, RVTY×9, GPRO×1006, MMED×53, SLN×83, OSCR×16, NVAX×49, BVS×35, BAK×266, EOSE×145, DELL×1 | 09:30 open · cash $166.91 (unchanged overnight, no fees) · equity $12,398.49 vs prior close $12,283.28 (+115.21) because holdings re-marked: CRK×102 yday $15.54 → 09:30 $15.45 -9.18; ANF×2 yday $136.60 → 09:30 $137.70 +2.20; BHVN×24 yday $15.69 → 09:30 $15.89 +4.80; BZ×22 yday $17.30 → 09:30 $17.31 +0.22; LVWR×296 yday $1.20 → 09:30 $1.17 -8.88; ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; VSTM×159 yday $8.02 → 09:30 $8.03 +1.59; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×1006 yday $1.69 → 09:30 $1.78 +90.54; MMED×53 yday $23.76 → 09:30 $23.88 +6.36; SLN×83 yday $14.79 → 09:30 $14.85 +4.98 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **BUY** | `BTSG` | 33 | $59.80 | $2.09 | — | $8,024.51 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=-5.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `IREN` | 43 | $45.98 | $2.12 | — | $6,045.25 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+12.3; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TPG` | 39 | $50.62 | $2.11 | — | $4,068.84 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+6.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `INO` | 2469 | $0.81 | $27.41 | — | $2,041.54 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+13.2; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-13 09:30 ET | **BUY** | `TNDM` | 85 | $23.33 | $2.25 | — | $56.25 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ⚪; ret5=+19.7; leftover $2000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $56.25 | ▲ 09:30 equity $10,321.25 vs yday $10,286.85 (+34.40) | 09:30 open · cash $56.25 (unchanged overnight, no fees) · equity $10,321.25 vs prior close $10,286.85 (+34.40) because holdings re-marked: BTSG×33 yday $60.23 → 09:30 $59.65 -19.14; IREN×43 yday $44.76 → 09:30 $44.09 -28.81; TPG×39 yday $54.62 → 09:30 $55.29 +26.13; INO×2469 yday $0.90 → 09:30 $0.93 +74.07; TNDM×85 yday $23.13 → 09:30 $22.92 -17.85 | — |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 7 | $0.94 | $0.09 | — | $49.60 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 4 | $1.50 | $0.07 | — | $43.53 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 1 | $4.31 | $0.05 | — | $39.18 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 1 | $4.18 | $0.04 | — | $34.95 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $7.03 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $10,645.20 vs yday $10,677.53 (-32.33) | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,645.20 vs prior close $10,677.53 (-32.33) because holdings re-marked: BTSG×33 yday $61.71 → 09:30 $61.69 -0.66; IREN×43 yday $44.06 → 09:30 $45.23 +50.31; TPG×39 yday $53.03 → 09:30 $52.67 -14.04; INO×2469 yday $1.09 → 09:30 $1.07 -49.38; TNDM×85 yday $22.72 → 09:30 $22.50 -18.70; LDI×7 yday $0.90 → 09:30 $0.91 +0.07; BTBT×4 yday $1.57 → 09:30 $1.52 -0.20; ANGX×1 yday $4.37 → 09:30 $4.60 +0.23; HYLN×1 yday $4.06 → 09:30 $4.10 +0.04 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $10,626.31 vs yday $10,729.57 (-103.26) | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,626.31 vs prior close $10,729.57 (-103.26) because holdings re-marked: BTSG×33 yday $60.38 → 09:30 $60.00 -12.54; IREN×43 yday $44.90 → 09:30 $43.56 -57.62; TPG×39 yday $51.77 → 09:30 $51.77 +0.00; INO×2469 yday $1.15 → 09:30 $1.14 -24.69; TNDM×85 yday $22.25 → 09:30 $22.16 -8.07; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×4 yday $1.60 → 09:30 $1.54 -0.24; ANGX×1 yday $4.71 → 09:30 $4.79 +0.08; HYLN×1 yday $4.09 → 09:30 $3.95 -0.14 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▲ 09:30 equity $10,928.57 vs yday $10,833.60 (+94.97) | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $10,928.57 vs prior close $10,833.60 (+94.97) because holdings re-marked: BTSG×33 yday $59.50 → 09:30 $60.15 +21.45; IREN×43 yday $42.00 → 09:30 $41.41 -25.16; TPG×39 yday $52.02 → 09:30 $52.26 +9.36; INO×2469 yday $1.20 → 09:30 $1.22 +49.38; TNDM×85 yday $23.73 → 09:30 $24.20 +39.95; LDI×7 yday $0.86 → 09:30 $0.88 +0.15; BTBT×4 yday $1.45 → 09:30 $1.42 -0.12; ANGX×1 yday $4.85 → 09:30 $4.79 -0.06; HYLN×1 yday $3.86 → 09:30 $3.87 +0.01 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $34.95 | ▼ 09:30 equity $11,059.34 vs yday $11,132.78 (-73.44) | 09:30 open · cash $34.95 (unchanged overnight, no fees) · equity $11,059.34 vs prior close $11,132.78 (-73.44) because holdings re-marked: BTSG×33 yday $59.33 → 09:30 $58.64 -22.77; IREN×43 yday $42.84 → 09:30 $42.46 -16.34; TPG×39 yday $53.18 → 09:30 $53.06 -4.68; INO×2469 yday $1.30 → 09:30 $1.30 +0.00; TNDM×85 yday $23.46 → 09:30 $23.11 -29.75; LDI×7 yday $0.88 → 09:30 $0.87 -0.04; BTBT×4 yday $1.40 → 09:30 $1.46 +0.22; ANGX×1 yday $4.60 → 09:30 $4.57 -0.03; HYLN×1 yday $3.67 → 09:30 $3.61 -0.06 | — |
| 2026-08-20 09:30 ET | **SELL** | `BTSG` | 33 | $58.64 | $2.11 | $-42.48 | $1,967.96 | ▼ -42.48 after sell → book $11,057.22; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `IREN` | 43 | $42.46 | $2.14 | $-155.62 | $3,791.59 | ▼ -155.62 after sell → book $11,055.08; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TPG` | 39 | $53.06 | $2.13 | $+90.79 | $5,858.80 | ▲ +90.79 after sell → book $11,052.95; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `INO` | 2469 | $1.30 | $32.29 | $+1150.12 | $9,036.21 | ▲ +1,150.12 after sell → book $11,020.66; vs 09:30 mark -32.29 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **SELL** | `TNDM` | 85 | $23.11 | $2.27 | $-23.22 | $10,998.29 | ▼ -23.22 after sell → book $11,018.39; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 66 | $20.55 | $2.19 | — | $9,639.80 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 66 | $20.65 | $2.19 | — | $8,274.71 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 238 | $5.77 | $3.07 | — | $6,898.38 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 70 | $19.63 | $2.20 | — | $5,522.08 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 46 | $29.63 | $2.13 | — | $4,156.97 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 785 | $1.75 | $10.13 | — | $2,773.10 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 9 | $144.54 | $2.02 | — | $1,470.22 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 279 | $4.92 | $3.60 | — | $93.94 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1374.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.94 | ▲ 09:30 equity $11,554.83 vs yday $11,169.22 (+385.61) | 09:30 open · cash $93.94 (unchanged overnight, no fees) · equity $11,554.83 vs prior close $11,169.22 (+385.61) because holdings re-marked: LDI×7 yday $0.87 → 09:30 $0.87 -0.02; BTBT×4 yday $1.59 → 09:30 $1.66 +0.26; ANGX×1 yday $4.37 → 09:30 $4.43 +0.06; HYLN×1 yday $3.37 → 09:30 $3.42 +0.05; AG×66 yday $21.19 → 09:30 $21.90 +46.86; CDE×66 yday $21.11 → 09:30 $21.75 +42.24; HDSN×238 yday $5.57 → 09:30 $5.67 +23.80; IAG×70 yday $20.50 → 09:30 $21.17 +46.90; KGC×46 yday $31.43 → 09:30 $32.17 +34.04; NFGC×785 yday $1.75 → 09:30 $1.79 +31.40; WPM×9 yday $150.25 → 09:30 $154.70 +40.05; ABUS×279 yday $4.77 → 09:30 $5.20 +119.97 | — |
| 2026-08-21 09:30 ET | **SELL** | `LDI` | 7 | $0.87 | $0.10 | $-0.68 | $99.91 | ▼ -0.68 after sell → book $11,554.73; vs 09:30 mark -0.10 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 1 | $4.43 | $0.07 | $+0.01 | $104.27 | ▲ +0.01 after sell → book $11,554.66; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 1 | $3.42 | $0.06 | $-0.86 | $107.63 | ▼ -0.86 after sell → book $11,554.60; vs 09:30 mark -0.06 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $96.39 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $15.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 11 | $1.32 | $0.18 | — | $81.69 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $15.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 1 | $14.96 | $0.15 | — | $66.58 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; ret5=-1.6; leftover $15.38 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $66.58 | ▲ 09:30 equity $11,659.70 vs yday $11,537.18 (+122.52) | 09:30 open · cash $66.58 (unchanged overnight, no fees) · equity $11,659.70 vs prior close $11,537.18 (+122.52) because holdings re-marked: BTBT×4 yday $1.53 → 09:30 $1.55 +0.08; AG×66 yday $21.09 → 09:30 $21.47 +25.08; CDE×66 yday $20.97 → 09:30 $21.26 +19.14; HDSN×238 yday $5.63 → 09:30 $5.69 +14.28; IAG×70 yday $21.14 → 09:30 $21.44 +21.00; KGC×46 yday $32.76 → 09:30 $33.21 +20.70; NFGC×785 yday $1.84 → 09:30 $1.86 +15.70; WPM×9 yday $157.78 → 09:30 $158.96 +10.62; ABUS×279 yday $5.21 → 09:30 $5.18 -8.37; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; CYPH×11 yday $1.42 → 09:30 $1.83 +4.51; QDEL×1 yday $14.74 → 09:30 $14.71 -0.03 | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 4 | $1.55 | $0.09 | $+0.03 | $72.69 | ▲ +0.03 after sell → book $11,659.61; vs 09:30 mark -0.09 | dropped from list after 6 sess (min 5) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $72.69 | ▲ 09:30 equity $11,608.59 vs yday $11,507.59 (+101.00) | 09:30 open · cash $72.69 (unchanged overnight, no fees) · equity $11,608.59 vs prior close $11,507.59 (+101.00) because holdings re-marked: AG×66 yday $20.57 → 09:30 $20.73 +10.56; CDE×66 yday $20.49 → 09:30 $20.85 +23.76; HDSN×238 yday $5.57 → 09:30 $5.53 -9.52; IAG×70 yday $21.36 → 09:30 $21.63 +18.90; KGC×46 yday $32.47 → 09:30 $32.76 +13.34; NFGC×785 yday $1.90 → 09:30 $1.91 +7.85; WPM×9 yday $158.00 → 09:30 $160.00 +18.00; ABUS×279 yday $5.20 → 09:30 $5.26 +16.74; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; CYPH×11 yday $1.64 → 09:30 $1.70 +0.66; QDEL×1 yday $14.36 → 09:30 $14.49 +0.13 | — |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 1 | $5.23 | $0.06 | — | $67.40 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+10.7; leftover $9.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 4 | $2.00 | $0.09 | — | $59.31 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $9.09 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 1 | $5.22 | $0.06 | — | $54.03 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $9.09 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 1 | $6.38 | $0.07 | — | $47.59 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $9.09 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.59 | ▲ 09:30 equity $11,525.26 vs yday $11,525.26 (-0.00) | 09:30 open · cash $47.59 (unchanged overnight, no fees) · equity $11,525.26 vs prior close $11,525.26 (-0.00) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.68 +0.00; CDE×66 yday $20.71 → 09:30 $20.71 +0.00; HDSN×238 yday $5.49 → 09:30 $5.49 +0.00; IAG×70 yday $21.48 → 09:30 $21.48 +0.00; KGC×46 yday $32.55 → 09:30 $32.55 +0.00; NFGC×785 yday $1.90 → 09:30 $1.90 +0.00; WPM×9 yday $158.25 → 09:30 $158.25 +0.00; ABUS×279 yday $5.20 → 09:30 $5.20 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; CYPH×11 yday $1.64 → 09:30 $1.64 +0.00; QDEL×1 yday $14.49 → 09:30 $14.49 +0.00; RZLT×1 yday $5.29 → 09:30 $5.29 +0.00; NPWR×4 yday $2.02 → 09:30 $2.02 +0.00; ALVO×1 yday $5.25 → 09:30 $5.25 +0.00; ZURA×1 yday $6.50 → 09:30 $6.50 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $47.59 | ▲ 09:30 equity $11,673.01 vs yday $11,608.32 (+64.69) | 09:30 open · cash $47.59 (unchanged overnight, no fees) · equity $11,673.01 vs prior close $11,608.32 (+64.69) because holdings re-marked: AG×66 yday $20.68 → 09:30 $20.63 -3.30; CDE×66 yday $20.71 → 09:30 $21.00 +19.14; HDSN×238 yday $5.49 → 09:30 $5.51 +4.76; IAG×70 yday $21.48 → 09:30 $21.64 +11.20; KGC×46 yday $32.55 → 09:30 $32.90 +16.10; NFGC×785 yday $1.90 → 09:30 $2.00 +78.50; WPM×9 yday $158.25 → 09:30 $160.93 +24.12; ABUS×279 yday $5.20 → 09:30 $5.19 -2.79; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; CYPH×11 yday $1.64 → 09:30 $1.60 -0.44; QDEL×1 yday $14.49 → 09:30 $15.09 +0.60; RZLT×1 yday $5.29 → 09:30 $5.01 -0.28; NPWR×4 yday $2.02 → 09:30 $1.93 -0.36; ALVO×1 yday $5.25 → 09:30 $4.98 -0.27; ZURA×1 yday $6.50 → 09:30 $6.13 -0.37 | — |
| 2026-08-27 09:30 ET | **SELL** | `AG` | 66 | $20.63 | $2.21 | $+0.88 | $1,406.96 | ▲ +0.88 after sell → book $11,670.80; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `CDE` | 66 | $21.00 | $2.21 | $+18.70 | $2,790.75 | ▲ +18.70 after sell → book $11,668.59; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `HDSN` | 238 | $5.51 | $3.12 | $-68.07 | $4,099.01 | ▼ -68.07 after sell → book $11,665.47; vs 09:30 mark -3.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `IAG` | 70 | $21.64 | $2.22 | $+136.28 | $5,611.58 | ▲ +136.28 after sell → book $11,663.24; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `KGC` | 46 | $32.90 | $2.15 | $+146.14 | $7,122.83 | ▲ +146.14 after sell → book $11,661.09; vs 09:30 mark -2.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NFGC` | 785 | $2.00 | $10.27 | $+175.85 | $8,682.56 | ▲ +175.85 after sell → book $11,650.82; vs 09:30 mark -10.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `WPM` | 9 | $160.93 | $2.04 | $+143.45 | $10,128.89 | ▲ +143.45 after sell → book $11,648.78; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `ABUS` | 279 | $5.19 | $3.66 | $+68.07 | $11,573.25 | ▲ +68.07 after sell → book $11,645.13; vs 09:30 mark -3.65 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 35 | $40.72 | $2.10 | — | $10,145.95 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+1.8; leftover $1446.66 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `CRK` | 102 | $14.09 | $2.30 | — | $8,706.48 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+1.1; leftover $1446.66 | join🟢 sector🔴 gen🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MOS` | 58 | $24.84 | $2.16 | — | $7,263.59 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+13.0; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🔴 buy🟢 |
| 2026-08-27 09:30 ET | **BUY** | `SLI` | 558 | $2.59 | $7.20 | — | $5,811.17 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; ret5=+4.2; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 7 | $190.90 | $2.01 | — | $4,472.86 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=-5.1; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 92 | $15.60 | $2.27 | — | $3,035.40 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+7.1; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GEN` | 50 | $28.89 | $2.14 | — | $1,588.76 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list mover_buy; 🔵; ret5=+1.6; leftover $1446.66 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,588.76 | ▲ 09:30 equity $11,769.16 vs yday $11,762.39 (+6.77) | 09:30 open · cash $1,588.76 (unchanged overnight, no fees) · equity $11,769.16 vs prior close $11,762.39 (+6.77) because holdings re-marked: ARCT×1 yday $15.83 → 09:30 $15.74 -0.09; CYPH×11 yday $1.63 → 09:30 $1.75 +1.32; QDEL×1 yday $14.91 → 09:30 $14.92 +0.01; RZLT×1 yday $5.04 → 09:30 $5.07 +0.03; NPWR×4 yday $1.81 → 09:30 $1.83 +0.08; ALVO×1 yday $4.91 → 09:30 $4.88 -0.03; ZURA×1 yday $5.99 → 09:30 $6.02 +0.03; RRC×35 yday $41.55 → 09:30 $41.44 -3.85; CRK×102 yday $14.50 → 09:30 $14.42 -8.16; MOS×58 yday $24.16 → 09:30 $24.00 -9.28; SLI×558 yday $2.61 → 09:30 $2.60 -5.58; ANET×7 yday $202.25 → 09:30 $205.90 +25.55; DLO×92 yday $15.36 → 09:30 $15.33 -2.76; GEN×50 yday $29.64 → 09:30 $29.83 +9.50 | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 1 | $15.74 | $0.18 | $+4.32 | $1,604.32 | ▲ +4.32 after sell → book $11,768.98; vs 09:30 mark -0.18 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 11 | $1.75 | $0.25 | $+4.31 | $1,623.32 | ▲ +4.31 after sell → book $11,768.73; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QDEL` | 1 | $14.92 | $0.17 | $-0.36 | $1,638.07 | ▼ -0.36 after sell → book $11,768.56; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 2 | $144.70 | $2.00 | — | $1,346.67 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $409.52 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 24 | $16.95 | $2.06 | — | $937.81 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $409.52 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 22 | $18.50 | $2.06 | — | $528.76 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $409.52 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 296 | $1.38 | $3.82 | — | $116.46 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $409.52 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.46 | ▼ 09:30 equity $11,632.86 vs yday $11,742.02 (-109.16) | 09:30 open · cash $116.46 (unchanged overnight, no fees) · equity $11,632.86 vs prior close $11,742.02 (-109.16) because holdings re-marked: RZLT×1 yday $4.98 → 09:30 $4.62 -0.36; NPWR×4 yday $1.89 → 09:30 $1.83 -0.24; ALVO×1 yday $4.88 → 09:30 $4.98 +0.10; ZURA×1 yday $5.85 → 09:30 $5.51 -0.34; RRC×35 yday $41.64 → 09:30 $41.11 -18.55; CRK×102 yday $14.62 → 09:30 $14.56 -6.12; MOS×58 yday $23.76 → 09:30 $23.75 -0.58; SLI×558 yday $2.64 → 09:30 $2.51 -72.54; ANET×7 yday $201.09 → 09:30 $199.00 -14.63; DLO×92 yday $15.14 → 09:30 $15.01 -11.96; GEN×50 yday $30.50 → 09:30 $31.02 +26.00; ANF×2 yday $145.75 → 09:30 $148.67 +5.84; BHVN×24 yday $16.12 → 09:30 $15.44 -16.32; BZ×22 yday $18.00 → 09:30 $17.89 -2.42; LVWR×296 yday $1.36 → 09:30 $1.37 +2.96 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $116.46 | ▼ 09:30 equity $11,615.69 vs yday $11,621.93 (-6.24) | 09:30 open · cash $116.46 (unchanged overnight, no fees) · equity $11,615.69 vs prior close $11,621.93 (-6.24) because holdings re-marked: RZLT×1 yday $4.62 → 09:30 $4.69 +0.07; NPWR×4 yday $1.82 → 09:30 $1.78 -0.16; ALVO×1 yday $4.96 → 09:30 $5.24 +0.28; ZURA×1 yday $5.64 → 09:30 $5.60 -0.04; RRC×35 yday $41.78 → 09:30 $41.32 -16.10; CRK×102 yday $14.51 → 09:30 $14.31 -20.40; MOS×58 yday $23.78 → 09:30 $24.00 +12.76; SLI×558 yday $2.51 → 09:30 $2.70 +106.02; ANET×7 yday $195.89 → 09:30 $196.60 +4.97; DLO×92 yday $15.00 → 09:30 $14.88 -11.04; GEN×50 yday $31.02 → 09:30 $30.56 -23.00; ANF×2 yday $149.28 → 09:30 $142.47 -13.62; BHVN×24 yday $15.40 → 09:30 $15.45 +1.20; BZ×22 yday $17.90 → 09:30 $17.37 -11.66; LVWR×296 yday $1.34 → 09:30 $1.22 -35.52 | — |
| 2026-09-01 09:30 ET | **SELL** | `RZLT` | 1 | $4.69 | $0.07 | $-0.67 | $121.08 | ▼ -0.67 after sell → book $11,615.62; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 4 | $1.78 | $0.10 | $-1.08 | $128.09 | ▼ -1.08 after sell → book $11,615.51; vs 09:30 mark -0.11 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 1 | $5.24 | $0.08 | $-0.11 | $133.26 | ▼ -0.11 after sell → book $11,615.44; vs 09:30 mark -0.07 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 1 | $5.60 | $0.08 | $-0.93 | $138.78 | ▼ -0.93 after sell → book $11,615.36; vs 09:30 mark -0.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.78 | ▲ 09:30 equity $11,735.69 vs yday $11,635.20 (+100.49) | 09:30 open · cash $138.78 (unchanged overnight, no fees) · equity $11,735.69 vs prior close $11,635.20 (+100.49) because holdings re-marked: RRC×35 yday $41.32 → 09:30 $41.94 +21.70; CRK×102 yday $14.90 → 09:30 $15.82 +93.84; MOS×58 yday $24.25 → 09:30 $23.94 -17.98; SLI×558 yday $2.70 → 09:30 $2.67 -16.74; ANET×7 yday $193.30 → 09:30 $195.77 +17.29; DLO×92 yday $14.70 → 09:30 $14.61 -8.28; GEN×50 yday $30.56 → 09:30 $30.73 +8.50; ANF×2 yday $143.00 → 09:30 $142.00 -2.00; BHVN×24 yday $15.45 → 09:30 $15.39 -1.44; BZ×22 yday $17.17 → 09:30 $17.29 +2.64; LVWR×296 yday $1.18 → 09:30 $1.19 +2.96 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $138.78 | ▼ 09:30 equity $11,614.34 vs yday $11,656.32 (-41.98) | 09:30 open · cash $138.78 (unchanged overnight, no fees) · equity $11,614.34 vs prior close $11,656.32 (-41.98) because holdings re-marked: RRC×35 yday $42.40 → 09:30 $42.10 -10.50; CRK×102 yday $16.02 → 09:30 $15.70 -32.64; MOS×58 yday $24.78 → 09:30 $24.70 -4.64; SLI×558 yday $2.49 → 09:30 $2.49 +0.00; ANET×7 yday $189.26 → 09:30 $188.00 -8.82; DLO×92 yday $14.83 → 09:30 $14.82 -0.92; GEN×50 yday $30.02 → 09:30 $30.04 +1.00; ANF×2 yday $140.68 → 09:30 $139.65 -2.06; BHVN×24 yday $15.74 → 09:30 $15.97 +5.52; BZ×22 yday $17.55 → 09:30 $17.65 +2.20; LVWR×296 yday $1.14 → 09:30 $1.17 +8.88 | — |
| 2026-09-03 09:30 ET | **SELL** | `RRC` | 35 | $42.10 | $2.12 | $+44.09 | $1,610.16 | ▲ +44.09 after sell → book $11,612.22; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `MOS` | 58 | $24.70 | $2.19 | $-12.47 | $3,040.58 | ▼ -12.47 after sell → book $11,610.04; vs 09:30 mark -2.18 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `SLI` | 558 | $2.49 | $7.30 | $-70.30 | $4,422.70 | ▼ -70.30 after sell → book $11,602.74; vs 09:30 mark -7.30 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `ANET` | 7 | $188.00 | $2.03 | $-24.34 | $5,736.66 | ▼ -24.34 after sell → book $11,600.70; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `DLO` | 92 | $14.82 | $2.29 | $-76.32 | $7,097.81 | ▼ -76.32 after sell → book $11,598.41; vs 09:30 mark -2.29 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **SELL** | `GEN` | 50 | $30.04 | $2.16 | $+53.20 | $8,597.65 | ▲ +53.20 after sell → book $11,596.25; vs 09:30 mark -2.16 | dropped from list after 5 sess (min 5) | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $7,401.35 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $6,201.28 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 159 | $7.70 | $2.47 | — | $4,974.51 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,839.04 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1006 | $1.22 | $12.98 | — | $2,598.74 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1228.24 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $1,389.25 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 83 | $14.70 | $2.24 | — | $166.91 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1228.24 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $166.91 | ▲ 09:30 equity $12,398.49 vs yday $12,283.28 (+115.21) | 09:30 open · cash $166.91 (unchanged overnight, no fees) · equity $12,398.49 vs prior close $12,283.28 (+115.21) because holdings re-marked: CRK×102 yday $15.54 → 09:30 $15.45 -9.18; ANF×2 yday $136.60 → 09:30 $137.70 +2.20; BHVN×24 yday $15.69 → 09:30 $15.89 +4.80; BZ×22 yday $17.30 → 09:30 $17.31 +0.22; LVWR×296 yday $1.20 → 09:30 $1.17 -8.88; ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; VSTM×159 yday $8.02 → 09:30 $8.03 +1.59; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×1006 yday $1.69 → 09:30 $1.78 +90.54; MMED×53 yday $23.76 → 09:30 $23.88 +6.36; SLN×83 yday $14.79 → 09:30 $14.85 +4.98 | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 102 | $15.45 | $2.33 | $+134.10 | $1,740.49 | ▲ +134.10 after sell → book $12,396.17; vs 09:30 mark -2.32 | dropped from list after 6 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 2 | $137.70 | $2.02 | $-18.01 | $2,013.87 | ▼ -18.01 after sell → book $12,394.15; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 24 | $15.89 | $2.08 | $-29.58 | $2,393.15 | ▼ -29.58 after sell → book $12,392.07; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 22 | $17.31 | $2.08 | $-30.31 | $2,771.89 | ▼ -30.31 after sell → book $12,389.99; vs 09:30 mark -2.08 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LVWR` | 296 | $1.17 | $3.88 | $-69.86 | $3,114.33 | ▼ -69.86 after sell → book $12,386.11; vs 09:30 mark -3.88 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 16 | $30.65 | $2.04 | — | $2,621.90 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ret5=-2.2; leftover $519.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 49 | $10.41 | $2.14 | — | $2,109.67 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $519.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 35 | $14.50 | $2.10 | — | $1,600.07 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $519.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 266 | $1.95 | $3.43 | — | $1,077.94 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $519.06 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 145 | $3.57 | $2.42 | — | $557.87 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $519.06 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 1 | $486.31 | $1.99 | — | $69.56 | — | union ∩ last_green hold 5, no 🚨; gate last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $519.06 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
