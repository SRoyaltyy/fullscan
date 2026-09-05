# Factor mine action — `union_blue_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ blue, no 🚨

Cash book **+8.13%** ($10,813) · signal-only (no cash/fees) was +9.21%. Starts YES **13/17**. Fills 130 · skips 53 · realized $+866.11.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $408.90.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | DVN, EOG, FANG, TMC, TGB, ABX, ALM, ALOY | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $152.39 | $9,984.67 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, ALOY×85 | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 |
| 2026-08-18 | -6.20 | $152.39 | DVN×27, EOG×8, FANG×6, TMC×309, TGB×147, ABX×137, ALM×77, ALOY×85 | $9,865.94 | -118.73 | — | DVN, EOG, FANG, TMC, TGB, ABX, ALM, ALOY | $9,846.32 | $9,846.32 | — | 09:30 open · cash $152.39 (unchanged overnight, no fees) · equity $9,865.94 vs prior close $9,984.67 (-118.73) because holdings re-marked: DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×309 yday $3.77 → 09:30 $3.72 -15.45; TGB×147 yday $8.77 → 09:30 $8.55 -32.34; ABX×137 yday $9.12 → 09:30 $9.03 -12.33; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; ALOY×85 yday $13.86 → 09:30 $13.19 -56.53 |
| 2026-08-19 | -7.20 | $9,846.32 | — | $9,846.32 | +0.00 | — | — | $9,846.32 | $9,846.32 | — | 09:30 open · cash $9,846.32 · no holdings · equity $9,846.32 vs prior close $9,846.32 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $9,846.32 | — | $9,846.32 | +0.00 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $160.44 | $10,051.62 | AG×59, BHP×13, CDE×59, HDSN×213, IAG×62, KGC×41, NFGC×703, WPM×8 | 09:30 open · cash $9,846.32 · no holdings · equity $9,846.32 vs prior close $9,846.32 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $160.44 | AG×59, BHP×13, CDE×59, HDSN×213, IAG×62, KGC×41, NFGC×703, WPM×8 | $10,315.34 | +263.72 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $313.95 | $10,509.84 | AU×10, AUPH×74, AEM×5, ARCT×115, AUTL×520, CRDL×666, CRSP×21, CYPH×974 | 09:30 open · cash $160.44 (unchanged overnight, no fees) · equity $10,315.34 vs prior close $10,051.62 (+263.72) because holdings re-marked: AG×59 yday $21.19 → 09:30 $21.90 +41.89; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×59 yday $21.11 → 09:30 $21.75 +37.76; HDSN×213 yday $5.57 → 09:30 $5.67 +21.30; IAG×62 yday $20.50 → 09:30 $21.17 +41.54; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×703 yday $1.75 → 09:30 $1.79 +28.12; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $313.95 | AU×10, AUPH×74, AEM×5, ARCT×115, AUTL×520, CRDL×666, CRSP×21, CYPH×974 | $10,847.03 | +337.19 | — | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $10,808.03 | $10,808.03 | — | 09:30 open · cash $313.95 (unchanged overnight, no fees) · equity $10,847.03 vs prior close $10,509.84 (+337.19) because holdings re-marked: AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×74 yday $16.65 → 09:30 $16.60 -3.70; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×115 yday $13.45 → 09:30 $13.26 -21.85; AUTL×520 yday $2.41 → 09:30 $2.36 -26.00; CRDL×666 yday $1.86 → 09:30 $1.87 +6.66; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×974 yday $1.42 → 09:30 $1.83 +399.34 |
| 2026-08-25 | +1.80 | $10,808.03 | — | $10,808.03 | +0.00 | OCUL, INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR | — | $46.26 | $10,939.83 | OCUL×123, INSP×21, CRMD×163, BMEA×833, NPWR×675, PUSA×365, ALVO×258, CAPR×198 | 09:30 open · cash $10,808.03 · no holdings · equity $10,808.03 vs prior close $10,808.03 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $46.26 | OCUL×123, INSP×21, CRMD×163, BMEA×833, NPWR×675, PUSA×365, ALVO×258, CAPR×198 | $10,939.83 | -0.00 | — | — | $46.26 | $10,771.07 | OCUL×123, INSP×21, CRMD×163, BMEA×833, NPWR×675, PUSA×365, ALVO×258, CAPR×198 | 09:30 open · cash $46.26 (unchanged overnight, no fees) · equity $10,939.83 vs prior close $10,939.83 (-0.00) because holdings re-marked: OCUL×123 yday $10.92 → 09:30 $10.92 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×163 yday $8.28 → 09:30 $8.28 +0.00; BMEA×833 yday $1.61 → 09:30 $1.61 +0.00; NPWR×675 yday $2.02 → 09:30 $2.02 +0.00; PUSA×365 yday $3.91 → 09:30 $3.91 +0.00; ALVO×258 yday $5.25 → 09:30 $5.25 +0.00; CAPR×198 yday $7.19 → 09:30 $7.19 +0.00 |
| 2026-08-27 | — | $46.26 | OCUL×123, INSP×21, CRMD×163, BMEA×833, NPWR×675, PUSA×365, ALVO×258, CAPR×198 | $11,125.06 | +353.99 | ACMR, GGB, MT, MU, TX, ANET, DLO | OCUL, INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR | $1,944.16 | $11,110.63 | ACMR×17, GGB×313, MT×18, MU×1, TX×25, ANET×7, DLO×88 | 09:30 open · cash $46.26 (unchanged overnight, no fees) · equity $11,125.06 vs prior close $10,771.07 (+353.99) because holdings re-marked: OCUL×123 yday $10.92 → 09:30 $10.79 -15.99; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×163 yday $8.28 → 09:30 $8.60 +52.16; BMEA×833 yday $1.61 → 09:30 $1.75 +116.62; NPWR×675 yday $2.02 → 09:30 $1.93 -60.75; PUSA×365 yday $3.91 → 09:30 $3.84 -25.55; ALVO×258 yday $5.25 → 09:30 $4.98 -69.66; CAPR×198 yday $7.19 → 09:30 $8.29 +217.80 |
| 2026-08-28 | +0.75 | $1,944.16 | ACMR×17, GGB×313, MT×18, MU×1, TX×25, ANET×7, DLO×88 | $11,242.94 | +132.31 | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | ACMR, GGB, MT, MU, TX, ANET, DLO | $270.84 | $11,029.40 | ANF×9, SEDG×41, SMTC×9, GRRR×88, URBN×16, VYX×156, TTMI×11, NVRI×60 | 09:30 open · cash $1,944.16 (unchanged overnight, no fees) · equity $11,242.94 vs prior close $11,110.63 (+132.31) because holdings re-marked: ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×313 yday $4.46 → 09:30 $4.57 +34.43; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61; TX×25 yday $55.13 → 09:30 $55.25 +3.00; ANET×7 yday $202.25 → 09:30 $205.90 +25.55; DLO×88 yday $15.36 → 09:30 $15.33 -2.64 |
| 2026-08-31 | -5.85 | $270.84 | ANF×9, SEDG×41, SMTC×9, GRRR×88, URBN×16, VYX×156, TTMI×11, NVRI×60 | $10,694.69 | -334.71 | — | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | $10,677.42 | $10,677.42 | — | 09:30 open · cash $270.84 (unchanged overnight, no fees) · equity $10,694.69 vs prior close $11,029.40 (-334.71) because holdings re-marked: ANF×9 yday $145.75 → 09:30 $148.67 +26.28; SEDG×41 yday $33.51 → 09:30 $31.50 -82.41; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; GRRR×88 yday $15.66 → 09:30 $14.32 -117.92; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; VYX×156 yday $9.18 → 09:30 $9.06 -18.72; TTMI×11 yday $124.73 → 09:30 $117.20 -82.83; NVRI×60 yday $22.47 → 09:30 $22.28 -11.40 |
| 2026-09-01 | -6.30 | $10,677.42 | — | $10,677.42 | -0.00 | — | — | $10,677.42 | $10,677.42 | — | 09:30 open · cash $10,677.42 · no holdings · equity $10,677.42 vs prior close $10,677.42 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,677.42 | — | $10,677.42 | -0.00 | — | — | $10,677.42 | $10,677.42 | — | 09:30 open · cash $10,677.42 · no holdings · equity $10,677.42 vs prior close $10,677.42 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,677.42 | — | $10,677.42 | -0.00 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MMED, CTMX | — | $125.61 | $11,048.63 | ATRC×26, HRMY×32, CABA×408, VSTM×173, RVTY×10, CRK×85, MMED×58, CTMX×358 | 09:30 open · cash $10,677.42 · no holdings · equity $10,677.42 vs prior close $10,677.42 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $125.61 | ATRC×26, HRMY×32, CABA×408, VSTM×173, RVTY×10, CRK×85, MMED×58, CTMX×358 | $11,102.61 | +53.98 | OSCR, BVS, GPRO, EOSE, SLBT, DELL | HRMY, VSTM, RVTY, CRK, MMED, CTMX | $408.90 | $10,813.08 | ATRC×26, CABA×408, OSCR×44, BVS×94, GPRO×770, EOSE×384, SLBT×446, DELL×2 | 09:30 open · cash $125.61 (unchanged overnight, no fees) · equity $11,102.61 vs prior close $11,048.63 (+53.98) because holdings re-marked: ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×408 yday $3.57 → 09:30 $3.63 +24.48; VSTM×173 yday $8.02 → 09:30 $8.03 +1.73; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; CRK×85 yday $15.54 → 09:30 $15.45 -7.65; MMED×58 yday $23.76 → 09:30 $23.88 +6.96; CTMX×358 yday $3.72 → 09:30 $3.73 +3.58 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.20 | ▲ 09:30 equity $10,054.84 vs yday $10,051.46 (+3.38) | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 | — |
| 2026-08-17 09:30 ET | **SELL** | `TLN` | 3 | $367.88 | $2.02 | $+20.13 | $1,661.82 | ▲ +20.13 after sell → book $10,052.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `VST` | 8 | $149.37 | $2.03 | $+15.71 | $2,854.74 | ▲ +15.71 after sell → book $10,050.79; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NRG` | 10 | $127.40 | $2.04 | $+69.94 | $4,126.70 | ▲ +69.94 after sell → book $10,048.75; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `DAVE` | 3 | $336.94 | $2.02 | $+14.07 | $5,135.50 | ▲ +14.07 after sell → book $10,046.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `SLG` | 21 | $55.37 | $2.07 | $-51.17 | $6,296.20 | ▼ -51.17 after sell → book $10,044.66; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MARA` | 138 | $9.22 | $2.44 | $+24.14 | $7,566.12 | ▲ +24.14 after sell → book $10,042.22; vs 09:30 mark -2.44 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `LDI` | 1334 | $0.91 | $16.33 | $-72.85 | $8,759.73 | ▼ -72.85 after sell → book $10,025.89; vs 09:30 mark -16.33 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $10,014.99 | ▼ -4.98 after sell → book $10,014.99; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 27 | $46.18 | $2.07 | — | $8,766.06 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+6.7; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `EOG` | 8 | $142.77 | $2.01 | — | $7,621.89 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+5.8; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `FANG` | 6 | $202.70 | $2.01 | — | $6,403.68 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+8.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 309 | $4.05 | $3.99 | — | $5,148.25 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 147 | $8.46 | $2.43 | — | $3,902.19 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 137 | $9.12 | $2.40 | — | $2,650.35 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 77 | $16.20 | $2.22 | — | $1,400.73 | — | union ∩ blue, no 🚨; gate blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 85 | $14.66 | $2.25 | — | $152.39 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $1251.87 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.39 | ▼ 09:30 equity $9,865.94 vs yday $9,984.67 (-118.73) | 09:30 open · cash $152.39 (unchanged overnight, no fees) · equity $9,865.94 vs prior close $9,984.67 (-118.73) because holdings re-marked: DVN×27 yday $47.57 → 09:30 $48.00 +11.61; EOG×8 yday $146.15 → 09:30 $148.04 +15.12; FANG×6 yday $206.29 → 09:30 $208.93 +15.84; TMC×309 yday $3.77 → 09:30 $3.72 -15.45; TGB×147 yday $8.77 → 09:30 $8.55 -32.34; ABX×137 yday $9.12 → 09:30 $9.03 -12.33; ALM×77 yday $16.36 → 09:30 $15.78 -44.66; ALOY×85 yday $13.86 → 09:30 $13.19 -56.53 | — |
| 2026-08-18 09:30 ET | **SELL** | `DVN` | 27 | $48.00 | $2.09 | $+44.98 | $1,446.30 | ▲ +44.98 after sell → book $9,863.85; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `EOG` | 8 | $148.04 | $2.03 | $+38.11 | $2,628.58 | ▲ +38.11 after sell → book $9,861.81; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FANG` | 6 | $208.93 | $2.03 | $+33.34 | $3,880.13 | ▲ +33.34 after sell → book $9,859.78; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TMC` | 309 | $3.72 | $4.05 | $-110.00 | $5,025.57 | ▼ -110.00 after sell → book $9,855.74; vs 09:30 mark -4.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `TGB` | 147 | $8.55 | $2.47 | $+8.33 | $6,279.95 | ▲ +8.33 after sell → book $9,853.27; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 137 | $9.03 | $2.43 | $-17.16 | $7,514.63 | ▼ -17.16 after sell → book $9,850.84; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ALM` | 77 | $15.78 | $2.24 | $-36.80 | $8,727.44 | ▼ -36.80 after sell → book $9,848.59; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `ALOY` | 85 | $13.19 | $2.27 | $-129.46 | $9,846.32 | ▼ -129.46 after sell → book $9,846.32; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,846.32 | ▲ 09:30 equity $9,846.32 vs yday $9,846.32 (+0.00) | 09:30 open · cash $9,846.32 · no holdings · equity $9,846.32 vs prior close $9,846.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,846.32 | ▲ 09:30 equity $9,846.32 vs yday $9,846.32 (+0.00) | 09:30 open · cash $9,846.32 · no holdings · equity $9,846.32 vs prior close $9,846.32 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 59 | $20.55 | $2.17 | — | $8,631.71 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,446.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 59 | $20.65 | $2.17 | — | $6,226.03 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 213 | $5.77 | $2.75 | — | $4,994.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 62 | $19.63 | $2.18 | — | $3,775.04 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 41 | $29.63 | $2.11 | — | $2,558.09 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 703 | $1.75 | $9.07 | — | $1,318.78 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $160.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1230.79 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $160.44 | ▲ 09:30 equity $10,315.34 vs yday $10,051.62 (+263.72) | 09:30 open · cash $160.44 (unchanged overnight, no fees) · equity $10,315.34 vs prior close $10,051.62 (+263.72) because holdings re-marked: AG×59 yday $21.19 → 09:30 $21.90 +41.89; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×59 yday $21.11 → 09:30 $21.75 +37.76; HDSN×213 yday $5.57 → 09:30 $5.67 +21.30; IAG×62 yday $20.50 → 09:30 $21.17 +41.54; KGC×41 yday $31.43 → 09:30 $32.17 +30.34; NFGC×703 yday $1.75 → 09:30 $1.79 +28.12; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 59 | $21.90 | $2.19 | $+75.30 | $1,450.35 | ▲ +75.30 after sell → book $10,313.15; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `BHP` | 13 | $95.72 | $2.05 | $+57.15 | $2,692.67 | ▲ +57.15 after sell → book $10,311.11; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 59 | $21.75 | $2.19 | $+60.55 | $3,973.73 | ▲ +60.55 after sell → book $10,308.92; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 213 | $5.67 | $2.79 | $-26.84 | $5,178.65 | ▼ -26.84 after sell → book $10,306.13; vs 09:30 mark -2.79 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 62 | $21.17 | $2.20 | $+91.11 | $6,488.99 | ▲ +91.11 after sell → book $10,303.93; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 41 | $32.17 | $2.13 | $+99.89 | $7,805.82 | ▲ +99.89 after sell → book $10,301.79; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 703 | $1.79 | $9.20 | $+9.86 | $9,055.00 | ▲ +9.86 after sell → book $10,292.60; vs 09:30 mark -9.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $10,290.57 | ▲ +77.23 after sell → book $10,290.57; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $9,094.25 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 74 | $17.20 | $2.21 | — | $7,819.23 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,735.73 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 115 | $11.13 | $2.33 | — | $5,453.44 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 520 | $2.47 | $6.71 | — | $4,162.34 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 666 | $1.93 | $8.59 | — | $2,868.36 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 21 | $59.72 | $2.05 | — | $1,612.19 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 974 | $1.32 | $12.56 | — | $313.95 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1286.32 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $313.95 | ▲ 09:30 equity $10,847.03 vs yday $10,509.84 (+337.19) | 09:30 open · cash $313.95 (unchanged overnight, no fees) · equity $10,847.03 vs prior close $10,509.84 (+337.19) because holdings re-marked: AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×74 yday $16.65 → 09:30 $16.60 -3.70; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×115 yday $13.45 → 09:30 $13.26 -21.85; AUTL×520 yday $2.41 → 09:30 $2.36 -26.00; CRDL×666 yday $1.86 → 09:30 $1.87 +6.66; CRSP×21 yday $59.50 → 09:30 $58.79 -14.91; CYPH×974 yday $1.42 → 09:30 $1.83 +399.34 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,516.91 | ▲ +6.64 after sell → book $10,844.99; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 74 | $16.60 | $2.23 | $-48.85 | $2,743.07 | ▼ -48.85 after sell → book $10,842.75; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $3,826.20 | ▼ -0.38 after sell → book $10,840.73; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 115 | $13.26 | $2.37 | $+240.25 | $5,348.73 | ▲ +240.25 after sell → book $10,838.36; vs 09:30 mark -2.37 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `AUTL` | 520 | $2.36 | $6.80 | $-70.71 | $6,569.13 | ▼ -70.71 after sell → book $10,831.56; vs 09:30 mark -6.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRDL` | 666 | $1.87 | $8.71 | $-57.26 | $7,805.83 | ▼ -57.26 after sell → book $10,822.84; vs 09:30 mark -8.72 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `CRSP` | 21 | $58.79 | $2.07 | $-23.66 | $9,038.35 | ▼ -23.66 after sell → book $10,820.77; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 974 | $1.83 | $12.74 | $+471.43 | $10,808.03 | ▲ +471.43 after sell → book $10,808.03; vs 09:30 mark -12.74 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,808.03 | ▲ 09:30 equity $10,808.03 vs yday $10,808.03 (+0.00) | 09:30 open · cash $10,808.03 · no holdings · equity $10,808.03 vs prior close $10,808.03 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 123 | $10.92 | $2.36 | — | $9,462.51 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+10.4; leftover $1351.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 21 | $61.47 | $2.05 | — | $8,169.59 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+9.2; leftover $1351.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 163 | $8.28 | $2.48 | — | $6,817.47 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1351.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 833 | $1.62 | $10.75 | — | $5,457.26 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1351.00 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 675 | $2.00 | $8.71 | — | $4,098.56 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1351.00 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 365 | $3.70 | $4.71 | — | $2,743.35 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1351.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 258 | $5.22 | $3.33 | — | $1,393.26 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1351.00 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 198 | $6.79 | $2.58 | — | $46.26 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1351.00 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.26 | ▲ 09:30 equity $10,939.83 vs yday $10,939.83 (-0.00) | 09:30 open · cash $46.26 (unchanged overnight, no fees) · equity $10,939.83 vs prior close $10,939.83 (-0.00) because holdings re-marked: OCUL×123 yday $10.92 → 09:30 $10.92 +0.00; INSP×21 yday $61.47 → 09:30 $61.47 +0.00; CRMD×163 yday $8.28 → 09:30 $8.28 +0.00; BMEA×833 yday $1.61 → 09:30 $1.61 +0.00; NPWR×675 yday $2.02 → 09:30 $2.02 +0.00; PUSA×365 yday $3.91 → 09:30 $3.91 +0.00; ALVO×258 yday $5.25 → 09:30 $5.25 +0.00; CAPR×198 yday $7.19 → 09:30 $7.19 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.26 | ▲ 09:30 equity $11,125.06 vs yday $10,771.07 (+353.99) | 09:30 open · cash $46.26 (unchanged overnight, no fees) · equity $11,125.06 vs prior close $10,771.07 (+353.99) because holdings re-marked: OCUL×123 yday $10.92 → 09:30 $10.79 -15.99; INSP×21 yday $61.47 → 09:30 $60.07 -29.40; CRMD×163 yday $8.28 → 09:30 $8.60 +52.16; BMEA×833 yday $1.61 → 09:30 $1.75 +116.62; NPWR×675 yday $2.02 → 09:30 $1.93 -60.75; PUSA×365 yday $3.91 → 09:30 $3.84 -25.55; ALVO×258 yday $5.25 → 09:30 $4.98 -69.66; CAPR×198 yday $7.19 → 09:30 $8.29 +217.80 | — |
| 2026-08-27 09:30 ET | **SELL** | `OCUL` | 123 | $10.79 | $2.39 | $-20.74 | $1,371.04 | ▼ -20.74 after sell → book $11,122.67; vs 09:30 mark -2.39 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `INSP` | 21 | $60.07 | $2.07 | $-33.53 | $2,630.43 | ▼ -33.53 after sell → book $11,120.59; vs 09:30 mark -2.08 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRMD` | 163 | $8.60 | $2.52 | $+47.16 | $4,029.72 | ▲ +47.16 after sell → book $11,118.08; vs 09:30 mark -2.51 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 833 | $1.75 | $10.90 | $+86.65 | $5,476.57 | ▲ +86.65 after sell → book $11,107.18; vs 09:30 mark -10.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 675 | $1.93 | $8.83 | $-64.79 | $6,770.49 | ▼ -64.79 after sell → book $11,098.35; vs 09:30 mark -8.83 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 365 | $3.84 | $4.78 | $+41.61 | $8,167.31 | ▲ +41.61 after sell → book $11,093.57; vs 09:30 mark -4.78 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 258 | $4.98 | $3.38 | $-68.63 | $9,448.77 | ▼ -68.63 after sell → book $11,090.19; vs 09:30 mark -3.38 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 198 | $8.29 | $2.63 | $+291.79 | $11,087.56 | ▲ +291.79 after sell → book $11,087.56; vs 09:30 mark -2.63 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **BUY** | `ACMR` | 17 | $80.97 | $2.04 | — | $9,709.03 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=-1.3; leftover $1385.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `GGB` | 313 | $4.42 | $4.04 | — | $8,321.53 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=-8.6; leftover $1385.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MT` | 18 | $75.12 | $2.04 | — | $6,967.33 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=-2.2; leftover $1385.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `MU` | 1 | $925.74 | $1.99 | — | $6,039.59 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=-0.5; leftover $1385.94 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `TX` | 25 | $55.20 | $2.06 | — | $4,657.53 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=+3.0; leftover $1385.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `ANET` | 7 | $190.90 | $2.01 | — | $3,319.22 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=-5.1; leftover $1385.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-27 09:30 ET | **BUY** | `DLO` | 88 | $15.60 | $2.25 | — | $1,944.16 | — | union ∩ blue, no 🚨; gate blue=True; list mover_buy; 🔵; ret5=+7.1; leftover $1385.94 | join🟢 sector🟢 gen🟢 digest🟢 judge🟡 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,944.16 | ▲ 09:30 equity $11,242.94 vs yday $11,110.63 (+132.31) | 09:30 open · cash $1,944.16 (unchanged overnight, no fees) · equity $11,242.94 vs prior close $11,110.63 (+132.31) because holdings re-marked: ACMR×17 yday $79.11 → 09:30 $81.65 +43.18; GGB×313 yday $4.46 → 09:30 $4.57 +34.43; MT×18 yday $74.53 → 09:30 $74.54 +0.18; MU×1 yday $938.40 → 09:30 $967.01 +28.61; TX×25 yday $55.13 → 09:30 $55.25 +3.00; ANET×7 yday $202.25 → 09:30 $205.90 +25.55; DLO×88 yday $15.36 → 09:30 $15.33 -2.64 | — |
| 2026-08-28 09:30 ET | **SELL** | `ACMR` | 17 | $81.65 | $2.06 | $+7.46 | $3,330.15 | ▲ +7.46 after sell → book $11,240.88; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `GGB` | 313 | $4.57 | $4.10 | $+38.81 | $4,756.46 | ▲ +38.81 after sell → book $11,236.78; vs 09:30 mark -4.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MT` | 18 | $74.54 | $2.06 | $-14.55 | $6,096.11 | ▼ -14.55 after sell → book $11,234.71; vs 09:30 mark -2.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `MU` | 1 | $967.01 | $2.01 | $+37.26 | $7,061.11 | ▲ +37.26 after sell → book $11,232.70; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `TX` | 25 | $55.25 | $2.09 | $-2.90 | $8,440.28 | ▼ -2.90 after sell → book $11,230.62; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `ANET` | 7 | $205.90 | $2.03 | $+100.96 | $9,879.54 | ▲ +100.96 after sell → book $11,228.58; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **SELL** | `DLO` | 88 | $15.33 | $2.28 | $-28.29 | $11,226.30 | ▼ -28.29 after sell → book $11,226.30; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,921.99 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1403.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $33.78 | $2.11 | — | $8,534.89 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1403.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $7,188.28 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1403.29 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 88 | $15.94 | $2.25 | — | $5,783.30 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1403.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $82.70 | $2.04 | — | $4,458.07 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1403.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 156 | $8.95 | $2.46 | — | $3,059.41 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=-3.1; leftover $1403.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 11 | $127.07 | $2.02 | — | $1,659.61 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1403.29 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVRI` | 60 | $23.11 | $2.17 | — | $270.84 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=+0.3; leftover $1403.29 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $270.84 | ▼ 09:30 equity $10,694.69 vs yday $11,029.40 (-334.71) | 09:30 open · cash $270.84 (unchanged overnight, no fees) · equity $10,694.69 vs prior close $11,029.40 (-334.71) because holdings re-marked: ANF×9 yday $145.75 → 09:30 $148.67 +26.28; SEDG×41 yday $33.51 → 09:30 $31.50 -82.41; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; GRRR×88 yday $15.66 → 09:30 $14.32 -117.92; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; VYX×156 yday $9.18 → 09:30 $9.06 -18.72; TTMI×11 yday $124.73 → 09:30 $117.20 -82.83; NVRI×60 yday $22.47 → 09:30 $22.28 -11.40 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $1,606.84 | ▲ +31.68 after sell → book $10,692.66; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 41 | $31.50 | $2.13 | $-97.73 | $2,896.20 | ▼ -97.73 after sell → book $10,690.52; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $133.04 | $2.04 | $-151.29 | $4,091.53 | ▼ -151.29 after sell → book $10,688.49; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 88 | $14.32 | $2.28 | $-147.09 | $5,349.41 | ▼ -147.09 after sell → book $10,686.21; vs 09:30 mark -2.28 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 16 | $81.09 | $2.06 | $-29.86 | $6,644.79 | ▼ -29.86 after sell → book $10,684.15; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `VYX` | 156 | $9.06 | $2.50 | $+12.21 | $8,055.65 | ▲ +12.21 after sell → book $10,681.65; vs 09:30 mark -2.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TTMI` | 11 | $117.20 | $2.04 | $-112.64 | $9,342.81 | ▼ -112.64 after sell → book $10,679.61; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `NVRI` | 60 | $22.28 | $2.19 | $-54.16 | $10,677.42 | ▼ -54.16 after sell → book $10,677.42; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,677.42 | ▲ 09:30 equity $10,677.42 vs yday $10,677.42 (-0.00) | 09:30 open · cash $10,677.42 · no holdings · equity $10,677.42 vs prior close $10,677.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,677.42 | ▲ 09:30 equity $10,677.42 vs yday $10,677.42 (-0.00) | 09:30 open · cash $10,677.42 · no holdings · equity $10,677.42 vs prior close $10,677.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,677.42 | ▲ 09:30 equity $10,677.42 vs yday $10,677.42 (-0.00) | 09:30 open · cash $10,677.42 · no holdings · equity $10,677.42 vs prior close $10,677.42 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 26 | $49.76 | $2.07 | — | $9,381.59 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1334.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 32 | $41.31 | $2.09 | — | $8,057.59 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1334.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 408 | $3.27 | $5.26 | — | $6,718.16 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1334.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 173 | $7.70 | $2.51 | — | $5,383.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1334.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 10 | $125.94 | $2.02 | — | $4,122.13 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1334.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 85 | $15.70 | $2.25 | — | $2,785.39 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1334.68 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 58 | $22.78 | $2.16 | — | $1,461.98 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1334.68 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 358 | $3.72 | $4.62 | — | $125.61 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1334.68 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $125.61 | ▲ 09:30 equity $11,102.61 vs yday $11,048.63 (+53.98) | 09:30 open · cash $125.61 (unchanged overnight, no fees) · equity $11,102.61 vs prior close $11,048.63 (+53.98) because holdings re-marked: ATRC×26 yday $52.59 → 09:30 $52.88 +7.54; HRMY×32 yday $42.86 → 09:30 $42.93 +2.24; CABA×408 yday $3.57 → 09:30 $3.63 +24.48; VSTM×173 yday $8.02 → 09:30 $8.03 +1.73; RVTY×10 yday $130.94 → 09:30 $132.45 +15.10; CRK×85 yday $15.54 → 09:30 $15.45 -7.65; MMED×58 yday $23.76 → 09:30 $23.88 +6.96; CTMX×358 yday $3.72 → 09:30 $3.73 +3.58 | — |
| 2026-09-04 09:30 ET | **SELL** | `HRMY` | 32 | $42.93 | $2.11 | $+47.65 | $1,497.26 | ▲ +47.65 after sell → book $11,100.50; vs 09:30 mark -2.11 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `VSTM` | 173 | $8.03 | $2.55 | $+52.03 | $2,883.90 | ▲ +52.03 after sell → book $11,097.95; vs 09:30 mark -2.55 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 10 | $132.45 | $2.04 | $+61.04 | $4,206.36 | ▲ +61.04 after sell → book $11,095.91; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 85 | $15.45 | $2.27 | $-25.76 | $5,517.34 | ▼ -25.76 after sell → book $11,093.64; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 58 | $23.88 | $2.19 | $+59.45 | $6,900.20 | ▲ +59.45 after sell → book $11,091.46; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 358 | $3.73 | $4.69 | $-5.73 | $8,230.85 | ▼ -5.73 after sell → book $11,086.77; vs 09:30 mark -4.69 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `OSCR` | 44 | $30.65 | $2.12 | — | $6,880.12 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=-2.2; leftover $1371.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 94 | $14.50 | $2.27 | — | $5,514.85 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1371.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GPRO` | 770 | $1.78 | $9.93 | — | $4,134.32 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $1371.81 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 384 | $3.57 | $4.95 | — | $2,758.49 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1371.81 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 446 | $3.07 | $5.75 | — | $1,383.51 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1371.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $408.90 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1371.81 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `INSP` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAPR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RZLT` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-27 | `ASML` | cash | leftover split 1385.94 < 1 share @ 1746.33 |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 26 | 2026-09-03 @ $49.76 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1334.68 |
| `CABA` | 408 | 2026-09-03 @ $3.27 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1334.68 |
| `OSCR` | 44 | 2026-09-04 @ $30.65 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=-2.2; leftover $1371.81 |
| `BVS` | 94 | 2026-09-04 @ $14.50 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $1371.81 |
| `GPRO` | 770 | 2026-09-04 @ $1.78 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $1371.81 |
| `EOSE` | 384 | 2026-09-04 @ $3.57 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1371.81 |
| `SLBT` | 446 | 2026-09-04 @ $3.07 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1371.81 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1371.81 |
