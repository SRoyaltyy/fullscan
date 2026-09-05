# Factor mine action — `union_blue_coil_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-2.31%** ($9,769) · signal-only (no cash/fees) was +3.07%. Starts YES **8/17**. Fills 90 · skips 146 · realized $-273.93.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `blue=True,ret_5_max=10.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $59.28.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | DVN, TMC, TGB, ABX, ALM, INV | — | $175.14 | $10,053.14 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 |
| 2026-08-18 | -6.20 | $175.14 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | $9,868.62 | -184.52 | — | — | $175.14 | $9,553.63 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | 09:30 open · cash $175.14 (unchanged overnight, no fees) · equity $9,868.62 vs prior close $10,053.14 (-184.52) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; DAVE×3 yday $341.43 → 09:30 $330.53 -32.70; SLG×21 yday $56.11 → 09:30 $56.00 -2.31; MARA×138 yday $9.72 → 09:30 $9.36 -49.68; LDI×1334 yday $0.88 → 09:30 $0.87 -6.67; BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×17 yday $3.77 → 09:30 $3.72 -0.85; TGB×8 yday $8.77 → 09:30 $8.55 -1.76; ABX×7 yday $9.12 → 09:30 $9.03 -0.63; ALM×4 yday $16.36 → 09:30 $15.78 -2.32; INV×43 yday $1.39 → 09:30 $1.32 -2.58 |
| 2026-08-19 | -7.20 | $175.14 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | $9,594.08 | +40.45 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $9,182.43 | $9,560.86 | DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | 09:30 open · cash $175.14 (unchanged overnight, no fees) · equity $9,594.08 vs prior close $9,553.63 (+40.45) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; DAVE×3 yday $333.14 → 09:30 $334.00 +2.58; SLG×21 yday $56.84 → 09:30 $57.50 +13.86; MARA×138 yday $8.96 → 09:30 $8.91 -6.90; LDI×1334 yday $0.86 → 09:30 $0.88 +29.35; BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×17 yday $3.92 → 09:30 $3.93 +0.17; TGB×8 yday $8.36 → 09:30 $8.70 +2.72; ABX×7 yday $9.01 → 09:30 $9.08 +0.49; ALM×4 yday $15.60 → 09:30 $16.05 +1.80; INV×43 yday $1.32 → 09:30 $1.39 +2.79 |
| 2026-08-20 | +1.12 | $9,182.43 | DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, INV×43 | $9,558.69 | -2.17 | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | DVN, TMC, TGB, ABX, ALM, INV | $62.39 | $9,693.06 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242 | 09:30 open · cash $9,182.43 (unchanged overnight, no fees) · equity $9,558.69 vs prior close $9,560.86 (-2.17) because holdings re-marked: DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×17 yday $3.97 → 09:30 $3.92 -0.85; TGB×8 yday $8.47 → 09:30 $8.35 -0.96; ABX×7 yday $9.15 → 09:30 $9.13 -0.14; ALM×4 yday $16.18 → 09:30 $15.81 -1.48; INV×43 yday $1.54 → 09:30 $1.55 +0.43 |
| 2026-08-21 | +3.25 | $62.39 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242 | $10,018.75 | +325.69 | BTBT | — | $55.67 | $10,062.92 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242, BTBT×4 | 09:30 open · cash $62.39 (unchanged overnight, no fees) · equity $10,018.75 vs prior close $9,693.06 (+325.69) because holdings re-marked: AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×206 yday $5.57 → 09:30 $5.67 +20.60; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×682 yday $1.75 → 09:30 $1.79 +27.28; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×242 yday $4.77 → 09:30 $5.20 +104.06 |
| 2026-08-24 | -5.17 | $55.67 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242, BTBT×4 | $10,153.25 | +90.33 | — | — | $55.67 | $10,057.57 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242, BTBT×4 | 09:30 open · cash $55.67 (unchanged overnight, no fees) · equity $10,153.25 vs prior close $10,062.92 (+90.33) because holdings re-marked: AG×58 yday $21.09 → 09:30 $21.47 +22.04; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; HDSN×206 yday $5.63 → 09:30 $5.69 +12.36; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×40 yday $32.76 → 09:30 $33.21 +18.00; NFGC×682 yday $1.84 → 09:30 $1.86 +13.64; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×242 yday $5.21 → 09:30 $5.18 -7.26; BTBT×4 yday $1.53 → 09:30 $1.55 +0.08 |
| 2026-08-25 | +1.80 | $55.67 | AG×58, BHP×13, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, ABUS×242, BTBT×4 | $10,114.48 | +56.91 | INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR, ALIT | AG, BHP, HDSN, IAG, KGC, NFGC, WPM, ABUS | $19.23 | $10,212.37 | BTBT×4, INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | 09:30 open · cash $55.67 (unchanged overnight, no fees) · equity $10,114.48 vs prior close $10,057.57 (+56.91) because holdings re-marked: AG×58 yday $20.57 → 09:30 $20.73 +9.28; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; HDSN×206 yday $5.57 → 09:30 $5.53 -8.24; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×40 yday $32.47 → 09:30 $32.76 +11.60; NFGC×682 yday $1.90 → 09:30 $1.91 +6.82; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×242 yday $5.20 → 09:30 $5.26 +14.52; BTBT×4 yday $1.56 → 09:30 $1.55 -0.04 |
| 2026-08-26 | +2.02 | $19.23 | BTBT×4, INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | $10,212.37 | -0.00 | — | — | $19.23 | $10,054.16 | BTBT×4, INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | 09:30 open · cash $19.23 (unchanged overnight, no fees) · equity $10,212.37 vs prior close $10,212.37 (-0.00) because holdings re-marked: BTBT×4 yday $1.53 → 09:30 $1.53 +0.00; INSP×20 yday $61.47 → 09:30 $61.47 +0.00; CRMD×152 yday $8.28 → 09:30 $8.28 +0.00; BMEA×778 yday $1.61 → 09:30 $1.61 +0.00; NPWR×630 yday $2.02 → 09:30 $2.02 +0.00; PUSA×340 yday $3.91 → 09:30 $3.91 +0.00; ALVO×241 yday $5.25 → 09:30 $5.25 +0.00; CAPR×185 yday $7.19 → 09:30 $7.19 +0.00; ALIT×84 yday $14.87 → 09:30 $14.87 +0.00 |
| 2026-08-27 | — | $19.23 | BTBT×4, INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | $10,398.18 | +344.02 | — | BTBT | $25.25 | $10,434.84 | INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | 09:30 open · cash $19.23 (unchanged overnight, no fees) · equity $10,398.18 vs prior close $10,054.16 (+344.02) because holdings re-marked: BTBT×4 yday $1.53 → 09:30 $1.53 +0.00; INSP×20 yday $61.47 → 09:30 $60.07 -28.00; CRMD×152 yday $8.28 → 09:30 $8.60 +48.64; BMEA×778 yday $1.61 → 09:30 $1.75 +108.92; NPWR×630 yday $2.02 → 09:30 $1.93 -56.70; PUSA×340 yday $3.91 → 09:30 $3.84 -23.80; ALVO×241 yday $5.25 → 09:30 $4.98 -65.07; CAPR×185 yday $7.19 → 09:30 $8.29 +203.50; ALIT×84 yday $14.87 → 09:30 $14.85 -1.68 |
| 2026-08-28 | +0.75 | $25.25 | INSP×20, CRMD×152, BMEA×778, NPWR×630, PUSA×340, ALVO×241, CAPR×185, ALIT×84 | $10,474.34 | +39.50 | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR, ALIT | $246.51 | $10,258.11 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | 09:30 open · cash $25.25 (unchanged overnight, no fees) · equity $10,474.34 vs prior close $10,434.84 (+39.50) because holdings re-marked: INSP×20 yday $61.80 → 09:30 $62.10 +6.00; CRMD×152 yday $8.39 → 09:30 $8.49 +15.20; BMEA×778 yday $1.71 → 09:30 $1.74 +23.34; NPWR×630 yday $1.81 → 09:30 $1.83 +12.60; PUSA×340 yday $3.85 → 09:30 $3.86 +3.40; ALVO×241 yday $4.91 → 09:30 $4.88 -7.23; CAPR×185 yday $9.36 → 09:30 $9.19 -31.45; ALIT×84 yday $14.33 → 09:30 $14.54 +17.64 |
| 2026-08-31 | -5.85 | $246.51 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | $9,955.51 | -302.60 | — | — | $246.51 | $9,945.24 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | 09:30 open · cash $246.51 (unchanged overnight, no fees) · equity $9,955.51 vs prior close $10,258.11 (-302.60) because holdings re-marked: ANF×9 yday $145.75 → 09:30 $148.67 +26.28; SEDG×38 yday $33.51 → 09:30 $31.50 -76.38; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×81 yday $15.66 → 09:30 $14.32 -108.54; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; VYX×145 yday $9.18 → 09:30 $9.06 -17.40; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; NVRI×56 yday $22.47 → 09:30 $22.28 -10.64 |
| 2026-09-01 | -6.30 | $246.51 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | $9,899.28 | -45.96 | — | — | $246.51 | $9,803.29 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | 09:30 open · cash $246.51 (unchanged overnight, no fees) · equity $9,899.28 vs prior close $9,945.24 (-45.96) because holdings re-marked: ANF×9 yday $149.28 → 09:30 $142.47 -61.29; SEDG×38 yday $31.27 → 09:30 $32.22 +36.10; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×81 yday $14.20 → 09:30 $15.05 +68.85; URBN×15 yday $81.09 → 09:30 $80.69 -6.00; VYX×145 yday $8.90 → 09:30 $8.40 -72.50; TTMI×10 yday $120.19 → 09:30 $119.79 -4.00; NVRI×56 yday $22.28 → 09:30 $22.28 +0.00 |
| 2026-09-02 | -3.83 | $246.51 | ANF×9, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×145, TTMI×10, NVRI×56 | $9,743.26 | -60.03 | — | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | $9,726.08 | $9,726.08 | — | 09:30 open · cash $246.51 (unchanged overnight, no fees) · equity $9,743.26 vs prior close $9,803.29 (-60.03) because holdings re-marked: ANF×9 yday $143.00 → 09:30 $142.00 -9.00; SEDG×38 yday $31.80 → 09:30 $31.87 +2.66; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×81 yday $14.80 → 09:30 $14.75 -4.05; URBN×15 yday $80.69 → 09:30 $79.12 -23.55; VYX×145 yday $8.27 → 09:30 $8.30 +4.35; TTMI×10 yday $116.94 → 09:30 $116.68 -2.60; NVRI×56 yday $22.28 → 09:30 $22.05 -12.88 |
| 2026-09-03 | -0.90 | $9,726.08 | — | $9,726.08 | -0.00 | HRMY, VSTM, RVTY, CRK, MMED, CTMX, CRDL, CLYM | — | $105.45 | $9,908.21 | HRMY×29, VSTM×157, RVTY×9, CRK×77, MMED×53, CTMX×326, CRDL×562, CLYM×82 | 09:30 open · cash $9,726.08 · no holdings · equity $9,726.08 vs prior close $9,726.08 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $105.45 | HRMY×29, VSTM×157, RVTY×9, CRK×77, MMED×53, CTMX×326, CRDL×562, CLYM×82 | $9,844.33 | -63.88 | GPRO, EOSE, SLBT, CCOI | — | $59.28 | $9,769.21 | HRMY×29, VSTM×157, RVTY×9, CRK×77, MMED×53, CTMX×326, CRDL×562, CLYM×82, GPRO×7, EOSE×3, SLBT×4, CCOI×1 | 09:30 open · cash $105.45 (unchanged overnight, no fees) · equity $9,844.33 vs prior close $9,908.21 (-63.88) because holdings re-marked: HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; VSTM×157 yday $8.02 → 09:30 $8.03 +1.57; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×77 yday $15.54 → 09:30 $15.45 -6.93; MMED×53 yday $23.76 → 09:30 $23.88 +6.36; CTMX×326 yday $3.72 → 09:30 $3.73 +3.26; CRDL×562 yday $2.17 → 09:30 $2.18 +5.62; CLYM×82 yday $15.05 → 09:30 $13.96 -89.38 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.20 | ▲ 09:30 equity $10,054.84 vs yday $10,051.46 (+3.38) | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $513.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+6.7; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 17 | $4.05 | $0.74 | — | $443.96 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=-12.3; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 8 | $8.46 | $0.70 | — | $375.58 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+0.4; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 7 | $9.12 | $0.66 | — | $311.08 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 4 | $16.20 | $0.66 | — | $245.62 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `INV` | 43 | $1.62 | $0.83 | — | $175.14 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_mover; 🔵; ⚪; ret5=-53.0; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.14 | ▼ 09:30 equity $9,868.62 vs yday $10,053.14 (-184.52) | 09:30 open · cash $175.14 (unchanged overnight, no fees) · equity $9,868.62 vs prior close $10,053.14 (-184.52) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; DAVE×3 yday $341.43 → 09:30 $330.53 -32.70; SLG×21 yday $56.11 → 09:30 $56.00 -2.31; MARA×138 yday $9.72 → 09:30 $9.36 -49.68; LDI×1334 yday $0.88 → 09:30 $0.87 -6.67; BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×17 yday $3.77 → 09:30 $3.72 -0.85; TGB×8 yday $8.77 → 09:30 $8.55 -1.76; ABX×7 yday $9.12 → 09:30 $9.03 -0.63; ALM×4 yday $16.36 → 09:30 $15.78 -2.32; INV×43 yday $1.39 → 09:30 $1.32 -2.58 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $175.14 | ▲ 09:30 equity $9,594.08 vs yday $9,553.63 (+40.45) | 09:30 open · cash $175.14 (unchanged overnight, no fees) · equity $9,594.08 vs prior close $9,553.63 (+40.45) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; DAVE×3 yday $333.14 → 09:30 $334.00 +2.58; SLG×21 yday $56.84 → 09:30 $57.50 +13.86; MARA×138 yday $8.96 → 09:30 $8.91 -6.90; LDI×1334 yday $0.86 → 09:30 $0.88 +29.35; BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×17 yday $3.92 → 09:30 $3.93 +0.17; TGB×8 yday $8.36 → 09:30 $8.70 +2.72; ABX×7 yday $9.01 → 09:30 $9.08 +0.49; ALM×4 yday $15.60 → 09:30 $16.05 +1.80; INV×43 yday $1.32 → 09:30 $1.39 +2.79 | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,136.12 | ▼ -120.51 after sell → book $9,592.06; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,260.00 | ▼ -53.33 after sell → book $9,590.02; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,419.96 | ▼ -42.06 after sell → book $9,587.98; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DAVE` | 3 | $334.00 | $2.02 | $+5.25 | $4,419.94 | ▲ +5.25 after sell → book $9,585.96; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 21 | $57.50 | $2.07 | $-6.44 | $5,625.37 | ▼ -6.44 after sell → book $9,583.89; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 138 | $8.91 | $2.44 | $-18.64 | $6,852.51 | ▼ -18.64 after sell → book $9,581.45; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 1334 | $0.88 | $15.97 | $-108.51 | $8,010.46 | ▼ -108.51 after sell → book $9,565.48; vs 09:30 mark -15.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $9,182.43 | ▼ -88.28 after sell → book $9,554.59; vs 09:30 mark -10.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,182.43 | ▼ 09:30 equity $9,558.69 vs yday $9,560.86 (-2.17) | 09:30 open · cash $9,182.43 (unchanged overnight, no fees) · equity $9,558.69 vs prior close $9,560.86 (-2.17) because holdings re-marked: DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×17 yday $3.97 → 09:30 $3.92 -0.85; TGB×8 yday $8.47 → 09:30 $8.35 -0.96; ABX×7 yday $9.15 → 09:30 $9.13 -0.14; ALM×4 yday $16.18 → 09:30 $15.81 -1.48; INV×43 yday $1.54 → 09:30 $1.55 +0.43 | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $9,230.93 | ▲ +1.86 after sell → book $9,558.17; vs 09:30 mark -0.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 17 | $3.92 | $0.74 | $-3.69 | $9,296.84 | ▼ -3.69 after sell → book $9,557.44; vs 09:30 mark -0.73 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 8 | $8.35 | $0.71 | $-2.29 | $9,362.92 | ▼ -2.29 after sell → book $9,556.72; vs 09:30 mark -0.72 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 7 | $9.13 | $0.68 | $-1.27 | $9,426.15 | ▼ -1.27 after sell → book $9,556.04; vs 09:30 mark -0.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 4 | $15.81 | $0.66 | $-2.88 | $9,488.73 | ▼ -2.88 after sell → book $9,555.38; vs 09:30 mark -0.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `INV` | 43 | $1.55 | $0.82 | $-4.65 | $9,554.56 | ▼ -4.65 after sell → book $9,554.56; vs 09:30 mark -0.82 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,360.50 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,175.34 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $5,984.06 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $4,804.09 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $3,616.78 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 682 | $1.75 | $8.80 | — | $2,414.49 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,256.15 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 242 | $4.92 | $3.12 | — | $62.39 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1194.32 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $62.39 | ▲ 09:30 equity $10,018.75 vs yday $9,693.06 (+325.69) | 09:30 open · cash $62.39 (unchanged overnight, no fees) · equity $10,018.75 vs prior close $9,693.06 (+325.69) because holdings re-marked: AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; HDSN×206 yday $5.57 → 09:30 $5.67 +20.60; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×682 yday $1.75 → 09:30 $1.79 +27.28; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×242 yday $4.77 → 09:30 $5.20 +104.06 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 4 | $1.66 | $0.08 | — | $55.67 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $7.80 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.67 | ▲ 09:30 equity $10,153.25 vs yday $10,062.92 (+90.33) | 09:30 open · cash $55.67 (unchanged overnight, no fees) · equity $10,153.25 vs prior close $10,062.92 (+90.33) because holdings re-marked: AG×58 yday $21.09 → 09:30 $21.47 +22.04; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; HDSN×206 yday $5.63 → 09:30 $5.69 +12.36; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×40 yday $32.76 → 09:30 $33.21 +18.00; NFGC×682 yday $1.84 → 09:30 $1.86 +13.64; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×242 yday $5.21 → 09:30 $5.18 -7.26; BTBT×4 yday $1.53 → 09:30 $1.55 +0.08 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $55.67 | ▲ 09:30 equity $10,114.48 vs yday $10,057.57 (+56.91) | 09:30 open · cash $55.67 (unchanged overnight, no fees) · equity $10,114.48 vs prior close $10,057.57 (+56.91) because holdings re-marked: AG×58 yday $20.57 → 09:30 $20.73 +9.28; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; HDSN×206 yday $5.57 → 09:30 $5.53 -8.24; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×40 yday $32.47 → 09:30 $32.76 +11.60; NFGC×682 yday $1.90 → 09:30 $1.91 +6.82; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×242 yday $5.20 → 09:30 $5.26 +14.52; BTBT×4 yday $1.56 → 09:30 $1.55 -0.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 58 | $20.73 | $2.18 | $+6.09 | $1,255.83 | ▲ +6.09 after sell → book $10,112.30; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,501.13 | ▲ +60.14 after sell → book $10,110.25; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 206 | $5.53 | $2.70 | $-54.80 | $3,637.61 | ▼ -54.80 after sell → book $10,107.55; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.63 | $2.19 | $+115.64 | $4,933.22 | ▲ +115.64 after sell → book $10,105.36; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 40 | $32.76 | $2.13 | $+120.96 | $6,241.49 | ▲ +120.96 after sell → book $10,103.23; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 682 | $1.91 | $8.92 | $+91.40 | $7,535.19 | ▲ +91.40 after sell → book $10,094.31; vs 09:30 mark -8.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $8,813.15 | ▲ +119.63 after sell → book $10,092.27; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 242 | $5.26 | $3.17 | $+75.99 | $10,082.90 | ▲ +75.99 after sell → book $10,089.10; vs 09:30 mark -3.17 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.47 | $2.05 | — | $8,851.45 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ret5=+9.2; leftover $1260.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 152 | $8.28 | $2.45 | — | $7,590.44 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1260.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 778 | $1.62 | $10.04 | — | $6,320.05 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1260.36 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 630 | $2.00 | $8.13 | — | $5,051.92 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1260.36 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 340 | $3.70 | $4.39 | — | $3,789.53 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1260.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 241 | $5.22 | $3.11 | — | $2,528.40 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1260.36 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 185 | $6.79 | $2.54 | — | $1,269.71 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1260.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 84 | $14.86 | $2.24 | — | $19.23 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1260.36 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.23 | ▲ 09:30 equity $10,212.37 vs yday $10,212.37 (-0.00) | 09:30 open · cash $19.23 (unchanged overnight, no fees) · equity $10,212.37 vs prior close $10,212.37 (-0.00) because holdings re-marked: BTBT×4 yday $1.53 → 09:30 $1.53 +0.00; INSP×20 yday $61.47 → 09:30 $61.47 +0.00; CRMD×152 yday $8.28 → 09:30 $8.28 +0.00; BMEA×778 yday $1.61 → 09:30 $1.61 +0.00; NPWR×630 yday $2.02 → 09:30 $2.02 +0.00; PUSA×340 yday $3.91 → 09:30 $3.91 +0.00; ALVO×241 yday $5.25 → 09:30 $5.25 +0.00; CAPR×185 yday $7.19 → 09:30 $7.19 +0.00; ALIT×84 yday $14.87 → 09:30 $14.87 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.23 | ▲ 09:30 equity $10,398.18 vs yday $10,054.16 (+344.02) | 09:30 open · cash $19.23 (unchanged overnight, no fees) · equity $10,398.18 vs prior close $10,054.16 (+344.02) because holdings re-marked: BTBT×4 yday $1.53 → 09:30 $1.53 +0.00; INSP×20 yday $61.47 → 09:30 $60.07 -28.00; CRMD×152 yday $8.28 → 09:30 $8.60 +48.64; BMEA×778 yday $1.61 → 09:30 $1.75 +108.92; NPWR×630 yday $2.02 → 09:30 $1.93 -56.70; PUSA×340 yday $3.91 → 09:30 $3.84 -23.80; ALVO×241 yday $5.25 → 09:30 $4.98 -65.07; CAPR×185 yday $7.19 → 09:30 $8.29 +203.50; ALIT×84 yday $14.87 → 09:30 $14.85 -1.68 | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 4 | $1.53 | $0.09 | $-0.69 | $25.25 | ▼ -0.69 after sell → book $10,398.08; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $25.25 | ▲ 09:30 equity $10,474.34 vs yday $10,434.84 (+39.50) | 09:30 open · cash $25.25 (unchanged overnight, no fees) · equity $10,474.34 vs prior close $10,434.84 (+39.50) because holdings re-marked: INSP×20 yday $61.80 → 09:30 $62.10 +6.00; CRMD×152 yday $8.39 → 09:30 $8.49 +15.20; BMEA×778 yday $1.71 → 09:30 $1.74 +23.34; NPWR×630 yday $1.81 → 09:30 $1.83 +12.60; PUSA×340 yday $3.85 → 09:30 $3.86 +3.40; ALVO×241 yday $4.91 → 09:30 $4.88 -7.23; CAPR×185 yday $9.36 → 09:30 $9.19 -31.45; ALIT×84 yday $14.33 → 09:30 $14.54 +17.64 | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $62.10 | $2.07 | $+8.48 | $1,265.18 | ▲ +8.48 after sell → book $10,472.27; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 152 | $8.49 | $2.48 | $+26.99 | $2,553.18 | ▲ +26.99 after sell → book $10,469.79; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 778 | $1.74 | $10.18 | $+73.15 | $3,896.73 | ▲ +73.15 after sell → book $10,459.62; vs 09:30 mark -10.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 630 | $1.83 | $8.24 | $-123.47 | $5,041.39 | ▼ -123.47 after sell → book $10,451.38; vs 09:30 mark -8.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 340 | $3.86 | $4.45 | $+45.56 | $6,349.33 | ▲ +45.56 after sell → book $10,446.92; vs 09:30 mark -4.46 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 241 | $4.88 | $3.16 | $-88.21 | $7,522.25 | ▼ -88.21 after sell → book $10,443.76; vs 09:30 mark -3.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 185 | $9.19 | $2.59 | $+438.87 | $9,219.81 | ▲ +438.87 after sell → book $10,441.17; vs 09:30 mark -2.59 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 84 | $14.54 | $2.27 | $-31.39 | $10,438.91 | ▼ -31.39 after sell → book $10,438.91; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,134.59 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1304.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $33.78 | $2.10 | — | $7,848.85 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,651.63 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 81 | $15.94 | $2.23 | — | $5,358.26 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $4,115.73 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1304.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 145 | $8.95 | $2.42 | — | $2,815.55 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=-3.1; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $1,542.83 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1304.86 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVRI` | 56 | $23.11 | $2.16 | — | $246.51 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+0.3; leftover $1304.86 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.51 | ▼ 09:30 equity $9,955.51 vs yday $10,258.11 (-302.60) | 09:30 open · cash $246.51 (unchanged overnight, no fees) · equity $9,955.51 vs prior close $10,258.11 (-302.60) because holdings re-marked: ANF×9 yday $145.75 → 09:30 $148.67 +26.28; SEDG×38 yday $33.51 → 09:30 $31.50 -76.38; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×81 yday $15.66 → 09:30 $14.32 -108.54; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; VYX×145 yday $9.18 → 09:30 $9.06 -17.40; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; NVRI×56 yday $22.47 → 09:30 $22.28 -10.64 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.51 | ▼ 09:30 equity $9,899.28 vs yday $9,945.24 (-45.96) | 09:30 open · cash $246.51 (unchanged overnight, no fees) · equity $9,899.28 vs prior close $9,945.24 (-45.96) because holdings re-marked: ANF×9 yday $149.28 → 09:30 $142.47 -61.29; SEDG×38 yday $31.27 → 09:30 $32.22 +36.10; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×81 yday $14.20 → 09:30 $15.05 +68.85; URBN×15 yday $81.09 → 09:30 $80.69 -6.00; VYX×145 yday $8.90 → 09:30 $8.40 -72.50; TTMI×10 yday $120.19 → 09:30 $119.79 -4.00; NVRI×56 yday $22.28 → 09:30 $22.28 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.51 | ▼ 09:30 equity $9,743.26 vs yday $9,803.29 (-60.03) | 09:30 open · cash $246.51 (unchanged overnight, no fees) · equity $9,743.26 vs prior close $9,803.29 (-60.03) because holdings re-marked: ANF×9 yday $143.00 → 09:30 $142.00 -9.00; SEDG×38 yday $31.80 → 09:30 $31.87 +2.66; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×81 yday $14.80 → 09:30 $14.75 -4.05; URBN×15 yday $80.69 → 09:30 $79.12 -23.55; VYX×145 yday $8.27 → 09:30 $8.30 +4.35; TTMI×10 yday $116.94 → 09:30 $116.68 -2.60; NVRI×56 yday $22.28 → 09:30 $22.05 -12.88 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $142.00 | $2.04 | $-28.35 | $1,522.47 | ▼ -28.35 after sell → book $9,741.22; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 38 | $31.87 | $2.12 | $-76.81 | $2,731.41 | ▼ -76.81 after sell → book $9,739.10; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $3,750.42 | ▼ -178.21 after sell → book $9,737.07; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 81 | $14.75 | $2.26 | $-100.88 | $4,942.91 | ▼ -100.88 after sell → book $9,734.81; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $79.12 | $2.06 | $-57.79 | $6,127.66 | ▼ -57.79 after sell → book $9,732.76; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 145 | $8.30 | $2.46 | $-99.13 | $7,328.70 | ▼ -99.13 after sell → book $9,730.30; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $116.68 | $2.04 | $-107.96 | $8,493.46 | ▼ -107.96 after sell → book $9,728.26; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NVRI` | 56 | $22.05 | $2.18 | $-63.70 | $9,726.08 | ▼ -63.70 after sell → book $9,726.08; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,726.08 | ▲ 09:30 equity $9,726.08 vs yday $9,726.08 (-0.00) | 09:30 open · cash $9,726.08 · no holdings · equity $9,726.08 vs prior close $9,726.08 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $8,526.01 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 157 | $7.70 | $2.46 | — | $7,314.65 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $6,179.17 | — | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 77 | $15.70 | $2.22 | — | $4,968.05 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1215.76 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 53 | $22.78 | $2.15 | — | $3,758.56 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 326 | $3.72 | $4.21 | — | $2,541.64 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 562 | $2.16 | $7.25 | — | $1,320.47 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CLYM` | 82 | $14.79 | $2.24 | — | $105.45 | — | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+5.8; leftover $1215.76 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $105.45 | ▼ 09:30 equity $9,844.33 vs yday $9,908.21 (-63.88) | 09:30 open · cash $105.45 (unchanged overnight, no fees) · equity $9,844.33 vs prior close $9,908.21 (-63.88) because holdings re-marked: HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; VSTM×157 yday $8.02 → 09:30 $8.03 +1.57; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×77 yday $15.54 → 09:30 $15.45 -6.93; MMED×53 yday $23.76 → 09:30 $23.88 +6.36; CTMX×326 yday $3.72 → 09:30 $3.73 +3.26; CRDL×562 yday $2.17 → 09:30 $2.18 +5.62; CLYM×82 yday $15.05 → 09:30 $13.96 -89.38 | — |
| 2026-09-04 09:30 ET | **BUY** | `GPRO` | 7 | $1.78 | $0.15 | — | $92.85 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $13.18 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 3 | $3.57 | $0.12 | — | $82.02 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $13.18 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 4 | $3.07 | $0.13 | — | $69.61 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $13.18 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 1 | $10.22 | $0.11 | — | $59.28 | — | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $13.18 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `TLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `VST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NRG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `DAVE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `SLG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MARA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `LDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `EOG` | cash | leftover split 70.02 < 1 share @ 142.77 |
| 2026-08-17 | `FANG` | cash | leftover split 70.02 < 1 share @ 202.70 |
| 2026-08-18 | `TLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `VST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NRG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DAVE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `SLG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MARA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `LDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `DVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `TGB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CRSP` | cash | leftover split 7.80 < 1 share @ 59.72 |
| 2026-08-21 | `FUTU` | cash | leftover split 7.80 < 1 share @ 115.18 |
| 2026-08-21 | `GMAB` | cash | leftover split 7.80 < 1 share @ 33.36 |
| 2026-08-21 | `MRVI` | cash | leftover split 7.80 < 1 share @ 8.20 |
| 2026-08-21 | `DE` | cash | leftover split 7.80 < 1 share @ 623.26 |
| 2026-08-21 | `WOLF` | cash | leftover split 7.80 < 1 share @ 26.86 |
| 2026-08-21 | `AMRC` | cash | leftover split 7.80 < 1 share @ 22.51 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BJ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `BZ` | no_price | no 09:30 open |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 3.16 < 1 share @ 80.97 |
| 2026-08-27 | `GGB` | cash | leftover split 3.16 < 1 share @ 4.42 |
| 2026-08-27 | `MT` | cash | leftover split 3.16 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 3.16 < 1 share @ 925.74 |
| 2026-08-27 | `TX` | cash | leftover split 3.16 < 1 share @ 55.20 |
| 2026-08-27 | `ANET` | cash | leftover split 3.16 < 1 share @ 190.90 |
| 2026-08-27 | `ASML` | cash | leftover split 3.16 < 1 share @ 1746.33 |
| 2026-08-27 | `DLO` | cash | leftover split 3.16 < 1 share @ 15.60 |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VYX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TTMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `NVRI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ZJYL` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `VYX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TTMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `NVRI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `OKE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OHI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BMRN` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PCRX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `HRMY` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `PBH` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `VSTM` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `MGTX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `HRMY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `VSTM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CLYM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `OSCR` | cash | leftover split 13.18 < 1 share @ 30.65 |
| 2026-09-04 | `BVS` | cash | leftover split 13.18 < 1 share @ 14.50 |
| 2026-09-04 | `DELL` | cash | leftover split 13.18 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 13.18 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `HRMY` | 29 | 2026-09-03 @ $41.31 | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1215.76 |
| `VSTM` | 157 | 2026-09-03 @ $7.70 | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1215.76 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | combo gate; gate blue=True,ret_5_max=10.0; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1215.76 |
| `CRK` | 77 | 2026-09-03 @ $15.70 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1215.76 |
| `MMED` | 53 | 2026-09-03 @ $22.78 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1215.76 |
| `CTMX` | 326 | 2026-09-03 @ $3.72 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1215.76 |
| `CRDL` | 562 | 2026-09-03 @ $2.16 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1215.76 |
| `CLYM` | 82 | 2026-09-03 @ $14.79 | combo gate; gate blue=True,ret_5_max=10.0; list yday_gainer; 🔵; ret5=+5.8; leftover $1215.76 |
| `GPRO` | 7 | 2026-09-04 @ $1.78 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $13.18 |
| `EOSE` | 3 | 2026-09-04 @ $3.57 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $13.18 |
| `SLBT` | 4 | 2026-09-04 @ $3.07 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $13.18 |
| `CCOI` | 1 | 2026-09-04 @ $10.22 | combo gate; gate blue=True,ret_5_max=10.0; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $13.18 |
