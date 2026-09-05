# Factor mine action — `union_blue_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ blue, no 🚨

Cash book **-1.53%** ($9,847) · signal-only (no cash/fees) was +8.43%. Starts YES **9/17**. Fills 94 · skips 152 · realized $-355.32.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $43.27.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | DVN, TMC, TGB, ABX, ALM, ALOY | — | $186.38 | $10,060.25 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, ALOY×4 | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 |
| 2026-08-18 | -6.20 | $186.38 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, ALOY×4 | $9,875.65 | -184.60 | — | — | $186.38 | $9,561.90 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, ALOY×4 | 09:30 open · cash $186.38 (unchanged overnight, no fees) · equity $9,875.65 vs prior close $10,060.25 (-184.60) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; DAVE×3 yday $341.43 → 09:30 $330.53 -32.70; SLG×21 yday $56.11 → 09:30 $56.00 -2.31; MARA×138 yday $9.72 → 09:30 $9.36 -49.68; LDI×1334 yday $0.88 → 09:30 $0.87 -6.67; BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×17 yday $3.77 → 09:30 $3.72 -0.85; TGB×8 yday $8.77 → 09:30 $8.55 -1.76; ABX×7 yday $9.12 → 09:30 $9.03 -0.63; ALM×4 yday $16.36 → 09:30 $15.78 -2.32; ALOY×4 yday $13.86 → 09:30 $13.19 -2.66 |
| 2026-08-19 | -7.20 | $186.38 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, ALOY×4 | $9,599.35 | +37.45 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $9,193.67 | $9,555.92 | DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, ALOY×4 | 09:30 open · cash $186.38 (unchanged overnight, no fees) · equity $9,599.35 vs prior close $9,561.90 (+37.45) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; DAVE×3 yday $333.14 → 09:30 $334.00 +2.58; SLG×21 yday $56.84 → 09:30 $57.50 +13.86; MARA×138 yday $8.96 → 09:30 $8.91 -6.90; LDI×1334 yday $0.86 → 09:30 $0.88 +29.35; BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×17 yday $3.92 → 09:30 $3.93 +0.17; TGB×8 yday $8.36 → 09:30 $8.70 +2.72; ABX×7 yday $9.01 → 09:30 $9.08 +0.49; ALM×4 yday $15.60 → 09:30 $16.05 +1.80; ALOY×4 yday $13.50 → 09:30 $13.45 -0.20 |
| 2026-08-20 | +1.12 | $9,193.67 | DVN×1, TMC×17, TGB×8, ABX×7, ALM×4, ALOY×4 | $9,551.52 | -4.40 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, TMC, TGB, ABX, ALM, ALOY | $71.84 | $9,749.69 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×681, WPM×8 | 09:30 open · cash $9,193.67 (unchanged overnight, no fees) · equity $9,551.52 vs prior close $9,555.92 (-4.40) because holdings re-marked: DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×17 yday $3.97 → 09:30 $3.92 -0.85; TGB×8 yday $8.47 → 09:30 $8.35 -0.96; ABX×7 yday $9.15 → 09:30 $9.13 -0.14; ALM×4 yday $16.18 → 09:30 $15.81 -1.48; ALOY×4 yday $12.51 → 09:30 $12.06 -1.80 |
| 2026-08-21 | +3.25 | $71.84 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×681, WPM×8 | $10,007.76 | +258.07 | AUTL, CRDL, CYPH | — | $48.52 | $10,005.47 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×681, WPM×8, AUTL×3, CRDL×4, CYPH×6 | 09:30 open · cash $71.84 (unchanged overnight, no fees) · equity $10,007.76 vs prior close $9,749.69 (+258.07) because holdings re-marked: AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×206 yday $5.57 → 09:30 $5.67 +20.60; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×681 yday $1.75 → 09:30 $1.79 +27.24; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $48.52 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×681, WPM×8, AUTL×3, CRDL×4, CYPH×6 | $10,121.84 | +116.37 | — | — | $48.52 | $9,975.99 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×681, WPM×8, AUTL×3, CRDL×4, CYPH×6 | 09:30 open · cash $48.52 (unchanged overnight, no fees) · equity $10,121.84 vs prior close $10,005.47 (+116.37) because holdings re-marked: AG×58 yday $21.09 → 09:30 $21.47 +22.04; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×57 yday $20.97 → 09:30 $21.26 +16.53; HDSN×206 yday $5.63 → 09:30 $5.69 +12.36; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×40 yday $32.76 → 09:30 $33.21 +18.00; NFGC×681 yday $1.84 → 09:30 $1.86 +13.62; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUTL×3 yday $2.41 → 09:30 $2.36 -0.15; CRDL×4 yday $1.86 → 09:30 $1.87 +0.04; CYPH×6 yday $1.42 → 09:30 $1.83 +2.46 |
| 2026-08-25 | +1.80 | $48.52 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×681, WPM×8, AUTL×3, CRDL×4, CYPH×6 | $10,039.51 | +63.52 | OCUL, INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $6.87 | $10,135.98 | AUTL×3, CRDL×4, CYPH×6, OCUL×114, INSP×20, CRMD×150, BMEA×770, NPWR×624, PUSA×337, ALVO×239, CAPR×183 | 09:30 open · cash $48.52 (unchanged overnight, no fees) · equity $10,039.51 vs prior close $9,975.99 (+63.52) because holdings re-marked: AG×58 yday $20.57 → 09:30 $20.73 +9.28; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×57 yday $20.49 → 09:30 $20.85 +20.52; HDSN×206 yday $5.57 → 09:30 $5.53 -8.24; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×40 yday $32.47 → 09:30 $32.76 +11.60; NFGC×681 yday $1.90 → 09:30 $1.91 +6.81; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUTL×3 yday $2.38 → 09:30 $2.32 -0.18; CRDL×4 yday $1.80 → 09:30 $1.90 +0.40; CYPH×6 yday $1.64 → 09:30 $1.70 +0.36 |
| 2026-08-26 | +2.02 | $6.87 | AUTL×3, CRDL×4, CYPH×6, OCUL×114, INSP×20, CRMD×150, BMEA×770, NPWR×624, PUSA×337, ALVO×239, CAPR×183 | $10,135.98 | -0.00 | — | — | $6.87 | $9,980.36 | AUTL×3, CRDL×4, CYPH×6, OCUL×114, INSP×20, CRMD×150, BMEA×770, NPWR×624, PUSA×337, ALVO×239, CAPR×183 | 09:30 open · cash $6.87 (unchanged overnight, no fees) · equity $10,135.98 vs prior close $10,135.98 (-0.00) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.34 +0.00; CRDL×4 yday $1.90 → 09:30 $1.90 +0.00; CYPH×6 yday $1.64 → 09:30 $1.64 +0.00; OCUL×114 yday $10.92 → 09:30 $10.92 +0.00; INSP×20 yday $61.47 → 09:30 $61.47 +0.00; CRMD×150 yday $8.28 → 09:30 $8.28 +0.00; BMEA×770 yday $1.61 → 09:30 $1.61 +0.00; NPWR×624 yday $2.02 → 09:30 $2.02 +0.00; PUSA×337 yday $3.91 → 09:30 $3.91 +0.00; ALVO×239 yday $5.25 → 09:30 $5.25 +0.00; CAPR×183 yday $7.19 → 09:30 $7.19 +0.00 |
| 2026-08-27 | — | $6.87 | AUTL×3, CRDL×4, CYPH×6, OCUL×114, INSP×20, CRMD×150, BMEA×770, NPWR×624, PUSA×337, ALVO×239, CAPR×183 | $10,306.47 | +326.11 | — | AUTL, CRDL, CYPH | $31.47 | $10,383.71 | OCUL×114, INSP×20, CRMD×150, BMEA×770, NPWR×624, PUSA×337, ALVO×239, CAPR×183 | 09:30 open · cash $6.87 (unchanged overnight, no fees) · equity $10,306.47 vs prior close $9,980.36 (+326.11) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.41 +0.21; CRDL×4 yday $1.90 → 09:30 $2.03 +0.52; CYPH×6 yday $1.64 → 09:30 $1.60 -0.24; OCUL×114 yday $10.92 → 09:30 $10.79 -14.82; INSP×20 yday $61.47 → 09:30 $60.07 -28.00; CRMD×150 yday $8.28 → 09:30 $8.60 +48.00; BMEA×770 yday $1.61 → 09:30 $1.75 +107.80; NPWR×624 yday $2.02 → 09:30 $1.93 -56.16; PUSA×337 yday $3.91 → 09:30 $3.84 -23.59; ALVO×239 yday $5.25 → 09:30 $4.98 -64.53; CAPR×183 yday $7.19 → 09:30 $8.29 +201.30 |
| 2026-08-28 | +0.75 | $31.47 | OCUL×114, INSP×20, CRMD×150, BMEA×770, NPWR×624, PUSA×337, ALVO×239, CAPR×183 | $10,389.42 | +5.71 | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | OCUL, INSP, CRMD, BMEA, NPWR, PUSA, ALVO, CAPR | $315.41 | $10,172.08 | ANF×8, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×144, TTMI×10, NVRI×56 | 09:30 open · cash $31.47 (unchanged overnight, no fees) · equity $10,389.42 vs prior close $10,383.71 (+5.71) because holdings re-marked: OCUL×114 yday $10.77 → 09:30 $10.63 -15.96; INSP×20 yday $61.80 → 09:30 $62.10 +6.00; CRMD×150 yday $8.39 → 09:30 $8.49 +15.00; BMEA×770 yday $1.71 → 09:30 $1.74 +23.10; NPWR×624 yday $1.81 → 09:30 $1.83 +12.48; PUSA×337 yday $3.85 → 09:30 $3.86 +3.37; ALVO×239 yday $4.91 → 09:30 $4.88 -7.17; CAPR×183 yday $9.36 → 09:30 $9.19 -31.11 |
| 2026-08-31 | -5.85 | $315.41 | ANF×8, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×144, TTMI×10, NVRI×56 | $9,866.68 | -305.40 | — | — | $315.41 | $9,855.96 | ANF×8, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×144, TTMI×10, NVRI×56 | 09:30 open · cash $315.41 (unchanged overnight, no fees) · equity $9,866.68 vs prior close $10,172.08 (-305.40) because holdings re-marked: ANF×8 yday $145.75 → 09:30 $148.67 +23.36; SEDG×38 yday $33.51 → 09:30 $31.50 -76.38; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×81 yday $15.66 → 09:30 $14.32 -108.54; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; VYX×144 yday $9.18 → 09:30 $9.06 -17.28; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; NVRI×56 yday $22.47 → 09:30 $22.28 -10.64 |
| 2026-09-01 | -6.30 | $315.41 | ANF×8, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×144, TTMI×10, NVRI×56 | $9,817.31 | -38.65 | — | — | $315.41 | $9,720.92 | ANF×8, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×144, TTMI×10, NVRI×56 | 09:30 open · cash $315.41 (unchanged overnight, no fees) · equity $9,817.31 vs prior close $9,855.96 (-38.65) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; SEDG×38 yday $31.27 → 09:30 $32.22 +36.10; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×81 yday $14.20 → 09:30 $15.05 +68.85; URBN×15 yday $81.09 → 09:30 $80.69 -6.00; VYX×144 yday $8.90 → 09:30 $8.40 -72.00; TTMI×10 yday $120.19 → 09:30 $119.79 -4.00; NVRI×56 yday $22.28 → 09:30 $22.28 +0.00 |
| 2026-09-02 | -3.83 | $315.41 | ANF×8, SEDG×38, SMTC×8, GRRR×81, URBN×15, VYX×144, TTMI×10, NVRI×56 | $9,661.86 | -59.06 | — | ANF, SEDG, SMTC, GRRR, URBN, VYX, TTMI, NVRI | $9,644.68 | $9,644.68 | — | 09:30 open · cash $315.41 (unchanged overnight, no fees) · equity $9,661.86 vs prior close $9,720.92 (-59.06) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; SEDG×38 yday $31.80 → 09:30 $31.87 +2.66; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×81 yday $14.80 → 09:30 $14.75 -4.05; URBN×15 yday $80.69 → 09:30 $79.12 -23.55; VYX×144 yday $8.27 → 09:30 $8.30 +4.32; TTMI×10 yday $116.94 → 09:30 $116.68 -2.60; NVRI×56 yday $22.28 → 09:30 $22.05 -12.88 |
| 2026-09-03 | -0.90 | $9,644.68 | — | $9,644.68 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY, CRK, MMED, CTMX | — | $109.49 | $9,979.77 | ATRC×24, HRMY×29, CABA×368, VSTM×156, RVTY×9, CRK×76, MMED×52, CTMX×324 | 09:30 open · cash $9,644.68 · no holdings · equity $9,644.68 vs prior close $9,644.68 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $109.49 | ATRC×24, HRMY×29, CABA×368, VSTM×156, RVTY×9, CRK×76, MMED×52, CTMX×324 | $10,028.63 | +48.86 | BVS, GPRO, EOSE, SLBT | — | $43.27 | $9,847.15 | ATRC×24, HRMY×29, CABA×368, VSTM×156, RVTY×9, CRK×76, MMED×52, CTMX×324, BVS×1, GPRO×10, EOSE×5, SLBT×5 | 09:30 open · cash $109.49 (unchanged overnight, no fees) · equity $10,028.63 vs prior close $9,979.77 (+48.86) because holdings re-marked: ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; CABA×368 yday $3.57 → 09:30 $3.63 +22.08; VSTM×156 yday $8.02 → 09:30 $8.03 +1.56; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×76 yday $15.54 → 09:30 $15.45 -6.84; MMED×52 yday $23.76 → 09:30 $23.88 +6.24; CTMX×324 yday $3.72 → 09:30 $3.73 +3.24 |

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
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $513.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+6.7; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 17 | $4.05 | $0.74 | — | $443.96 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 8 | $8.46 | $0.70 | — | $375.58 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 7 | $9.12 | $0.66 | — | $311.08 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 4 | $16.20 | $0.66 | — | $245.62 | — | union ∩ blue, no 🚨; gate blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALOY` | 4 | $14.66 | $0.60 | — | $186.38 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.38 | ▼ 09:30 equity $9,875.65 vs yday $10,060.25 (-184.60) | 09:30 open · cash $186.38 (unchanged overnight, no fees) · equity $9,875.65 vs prior close $10,060.25 (-184.60) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; DAVE×3 yday $341.43 → 09:30 $330.53 -32.70; SLG×21 yday $56.11 → 09:30 $56.00 -2.31; MARA×138 yday $9.72 → 09:30 $9.36 -49.68; LDI×1334 yday $0.88 → 09:30 $0.87 -6.67; BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×17 yday $3.77 → 09:30 $3.72 -0.85; TGB×8 yday $8.77 → 09:30 $8.55 -1.76; ABX×7 yday $9.12 → 09:30 $9.03 -0.63; ALM×4 yday $16.36 → 09:30 $15.78 -2.32; ALOY×4 yday $13.86 → 09:30 $13.19 -2.66 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $186.38 | ▲ 09:30 equity $9,599.35 vs yday $9,561.90 (+37.45) | 09:30 open · cash $186.38 (unchanged overnight, no fees) · equity $9,599.35 vs prior close $9,561.90 (+37.45) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; DAVE×3 yday $333.14 → 09:30 $334.00 +2.58; SLG×21 yday $56.84 → 09:30 $57.50 +13.86; MARA×138 yday $8.96 → 09:30 $8.91 -6.90; LDI×1334 yday $0.86 → 09:30 $0.88 +29.35; BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×17 yday $3.92 → 09:30 $3.93 +0.17; TGB×8 yday $8.36 → 09:30 $8.70 +2.72; ABX×7 yday $9.01 → 09:30 $9.08 +0.49; ALM×4 yday $15.60 → 09:30 $16.05 +1.80; ALOY×4 yday $13.50 → 09:30 $13.45 -0.20 | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,147.36 | ▼ -120.51 after sell → book $9,597.33; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,271.25 | ▼ -53.33 after sell → book $9,595.30; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,431.21 | ▼ -42.06 after sell → book $9,593.26; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DAVE` | 3 | $334.00 | $2.02 | $+5.25 | $4,431.19 | ▲ +5.25 after sell → book $9,591.24; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 21 | $57.50 | $2.07 | $-6.44 | $5,636.62 | ▼ -6.44 after sell → book $9,589.17; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 138 | $8.91 | $2.44 | $-18.64 | $6,863.76 | ▼ -18.64 after sell → book $9,586.73; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 1334 | $0.88 | $15.97 | $-108.51 | $8,021.71 | ▼ -108.51 after sell → book $9,570.76; vs 09:30 mark -15.97 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $9,193.67 | ▼ -88.28 after sell → book $9,559.86; vs 09:30 mark -10.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,193.67 | ▼ 09:30 equity $9,551.52 vs yday $9,555.92 (-4.40) | 09:30 open · cash $9,193.67 (unchanged overnight, no fees) · equity $9,551.52 vs prior close $9,555.92 (-4.40) because holdings re-marked: DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×17 yday $3.97 → 09:30 $3.92 -0.85; TGB×8 yday $8.47 → 09:30 $8.35 -0.96; ABX×7 yday $9.15 → 09:30 $9.13 -0.14; ALM×4 yday $16.18 → 09:30 $15.81 -1.48; ALOY×4 yday $12.51 → 09:30 $12.06 -1.80 | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $9,242.18 | ▲ +1.86 after sell → book $9,551.01; vs 09:30 mark -0.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 17 | $3.92 | $0.74 | $-3.69 | $9,308.08 | ▼ -3.69 after sell → book $9,550.27; vs 09:30 mark -0.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 8 | $8.35 | $0.71 | $-2.29 | $9,374.17 | ▼ -2.29 after sell → book $9,549.56; vs 09:30 mark -0.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 7 | $9.13 | $0.68 | $-1.27 | $9,437.40 | ▼ -1.27 after sell → book $9,548.88; vs 09:30 mark -0.68 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 4 | $15.81 | $0.66 | $-2.88 | $9,499.98 | ▼ -2.88 after sell → book $9,548.22; vs 09:30 mark -0.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALOY` | 4 | $12.06 | $0.51 | $-11.51 | $9,547.70 | ▼ -11.51 after sell → book $9,547.70; vs 09:30 mark -0.52 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,353.64 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1193.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,168.48 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1193.46 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,989.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1193.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $4,797.99 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1193.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,618.02 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1193.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,430.71 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1193.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 681 | $1.75 | $8.78 | — | $1,230.18 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1193.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $71.84 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1193.46 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.84 | ▲ 09:30 equity $10,007.76 vs yday $9,749.69 (+258.07) | 09:30 open · cash $71.84 (unchanged overnight, no fees) · equity $10,007.76 vs prior close $9,749.69 (+258.07) because holdings re-marked: AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×206 yday $5.57 → 09:30 $5.67 +20.60; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×681 yday $1.75 → 09:30 $1.79 +27.24; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 3 | $2.47 | $0.08 | — | $64.35 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $8.98 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 4 | $1.93 | $0.09 | — | $56.54 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $8.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 6 | $1.32 | $0.10 | — | $48.52 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $8.98 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.52 | ▲ 09:30 equity $10,121.84 vs yday $10,005.47 (+116.37) | 09:30 open · cash $48.52 (unchanged overnight, no fees) · equity $10,121.84 vs prior close $10,005.47 (+116.37) because holdings re-marked: AG×58 yday $21.09 → 09:30 $21.47 +22.04; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×57 yday $20.97 → 09:30 $21.26 +16.53; HDSN×206 yday $5.63 → 09:30 $5.69 +12.36; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×40 yday $32.76 → 09:30 $33.21 +18.00; NFGC×681 yday $1.84 → 09:30 $1.86 +13.62; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUTL×3 yday $2.41 → 09:30 $2.36 -0.15; CRDL×4 yday $1.86 → 09:30 $1.87 +0.04; CYPH×6 yday $1.42 → 09:30 $1.83 +2.46 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $48.52 | ▲ 09:30 equity $10,039.51 vs yday $9,975.99 (+63.52) | 09:30 open · cash $48.52 (unchanged overnight, no fees) · equity $10,039.51 vs prior close $9,975.99 (+63.52) because holdings re-marked: AG×58 yday $20.57 → 09:30 $20.73 +9.28; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×57 yday $20.49 → 09:30 $20.85 +20.52; HDSN×206 yday $5.57 → 09:30 $5.53 -8.24; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×40 yday $32.47 → 09:30 $32.76 +11.60; NFGC×681 yday $1.90 → 09:30 $1.91 +6.81; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUTL×3 yday $2.38 → 09:30 $2.32 -0.18; CRDL×4 yday $1.80 → 09:30 $1.90 +0.40; CYPH×6 yday $1.64 → 09:30 $1.70 +0.36 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 58 | $20.73 | $2.18 | $+6.09 | $1,248.68 | ▲ +6.09 after sell → book $10,037.33; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,493.98 | ▲ +60.14 after sell → book $10,035.28; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.85 | $2.18 | $+7.06 | $3,680.25 | ▲ +7.06 after sell → book $10,033.10; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 206 | $5.53 | $2.70 | $-54.80 | $4,816.73 | ▼ -54.80 after sell → book $10,030.40; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.63 | $2.19 | $+115.64 | $6,112.34 | ▲ +115.64 after sell → book $10,028.21; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 40 | $32.76 | $2.13 | $+120.96 | $7,420.61 | ▲ +120.96 after sell → book $10,026.08; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 681 | $1.91 | $8.91 | $+91.27 | $8,712.41 | ▲ +91.27 after sell → book $10,017.17; vs 09:30 mark -8.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $9,990.37 | ▲ +119.63 after sell → book $10,015.13; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 114 | $10.92 | $2.33 | — | $8,743.16 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+10.4; leftover $1248.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.47 | $2.05 | — | $7,511.71 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ret5=+9.2; leftover $1248.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 150 | $8.28 | $2.44 | — | $6,267.27 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1248.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 770 | $1.62 | $9.93 | — | $5,009.94 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1248.80 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 624 | $2.00 | $8.05 | — | $3,753.89 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1248.80 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 337 | $3.70 | $4.35 | — | $2,502.64 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1248.80 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 239 | $5.22 | $3.08 | — | $1,251.98 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1248.80 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 183 | $6.79 | $2.54 | — | $6.87 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1248.80 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.87 | ▲ 09:30 equity $10,135.98 vs yday $10,135.98 (-0.00) | 09:30 open · cash $6.87 (unchanged overnight, no fees) · equity $10,135.98 vs prior close $10,135.98 (-0.00) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.34 +0.00; CRDL×4 yday $1.90 → 09:30 $1.90 +0.00; CYPH×6 yday $1.64 → 09:30 $1.64 +0.00; OCUL×114 yday $10.92 → 09:30 $10.92 +0.00; INSP×20 yday $61.47 → 09:30 $61.47 +0.00; CRMD×150 yday $8.28 → 09:30 $8.28 +0.00; BMEA×770 yday $1.61 → 09:30 $1.61 +0.00; NPWR×624 yday $2.02 → 09:30 $2.02 +0.00; PUSA×337 yday $3.91 → 09:30 $3.91 +0.00; ALVO×239 yday $5.25 → 09:30 $5.25 +0.00; CAPR×183 yday $7.19 → 09:30 $7.19 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $6.87 | ▲ 09:30 equity $10,306.47 vs yday $9,980.36 (+326.11) | 09:30 open · cash $6.87 (unchanged overnight, no fees) · equity $10,306.47 vs prior close $9,980.36 (+326.11) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.41 +0.21; CRDL×4 yday $1.90 → 09:30 $2.03 +0.52; CYPH×6 yday $1.64 → 09:30 $1.60 -0.24; OCUL×114 yday $10.92 → 09:30 $10.79 -14.82; INSP×20 yday $61.47 → 09:30 $60.07 -28.00; CRMD×150 yday $8.28 → 09:30 $8.60 +48.00; BMEA×770 yday $1.61 → 09:30 $1.75 +107.80; NPWR×624 yday $2.02 → 09:30 $1.93 -56.16; PUSA×337 yday $3.91 → 09:30 $3.84 -23.59; ALVO×239 yday $5.25 → 09:30 $4.98 -64.53; CAPR×183 yday $7.19 → 09:30 $8.29 +201.30 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 3 | $2.41 | $0.10 | $-0.36 | $14.00 | ▼ -0.36 after sell → book $10,306.37; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 4 | $2.03 | $0.11 | $+0.20 | $22.01 | ▲ +0.20 after sell → book $10,306.26; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 6 | $1.60 | $0.13 | $+1.45 | $31.47 | ▲ +1.45 after sell → book $10,306.12; vs 09:30 mark -0.14 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $31.47 | ▲ 09:30 equity $10,389.42 vs yday $10,383.71 (+5.71) | 09:30 open · cash $31.47 (unchanged overnight, no fees) · equity $10,389.42 vs prior close $10,383.71 (+5.71) because holdings re-marked: OCUL×114 yday $10.77 → 09:30 $10.63 -15.96; INSP×20 yday $61.80 → 09:30 $62.10 +6.00; CRMD×150 yday $8.39 → 09:30 $8.49 +15.00; BMEA×770 yday $1.71 → 09:30 $1.74 +23.10; NPWR×624 yday $1.81 → 09:30 $1.83 +12.48; PUSA×337 yday $3.85 → 09:30 $3.86 +3.37; ALVO×239 yday $4.91 → 09:30 $4.88 -7.17; CAPR×183 yday $9.36 → 09:30 $9.19 -31.11 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 114 | $10.63 | $2.36 | $-37.75 | $1,240.93 | ▼ -37.75 after sell → book $10,387.06; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $62.10 | $2.07 | $+8.48 | $2,480.86 | ▲ +8.48 after sell → book $10,384.99; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 150 | $8.49 | $2.48 | $+26.58 | $3,751.89 | ▲ +26.58 after sell → book $10,382.52; vs 09:30 mark -2.47 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 770 | $1.74 | $10.07 | $+72.40 | $5,081.61 | ▲ +72.40 after sell → book $10,372.44; vs 09:30 mark -10.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 624 | $1.83 | $8.16 | $-122.29 | $6,215.37 | ▼ -122.29 after sell → book $10,364.28; vs 09:30 mark -8.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 337 | $3.86 | $4.41 | $+45.16 | $7,511.78 | ▲ +45.16 after sell → book $10,359.87; vs 09:30 mark -4.41 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 239 | $4.88 | $3.13 | $-87.48 | $8,674.96 | ▼ -87.48 after sell → book $10,356.73; vs 09:30 mark -3.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 183 | $9.19 | $2.58 | $+434.08 | $10,354.15 | ▲ +434.08 after sell → book $10,354.15; vs 09:30 mark -2.58 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $9,194.54 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1294.27 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 38 | $33.78 | $2.10 | — | $7,908.79 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1294.27 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $6,711.58 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1294.27 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 81 | $15.94 | $2.23 | — | $5,418.21 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1294.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $4,175.67 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1294.27 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `VYX` | 144 | $8.95 | $2.42 | — | $2,884.45 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=-3.1; leftover $1294.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TTMI` | 10 | $127.07 | $2.02 | — | $1,611.73 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=-21.0; leftover $1294.27 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `NVRI` | 56 | $23.11 | $2.16 | — | $315.41 | — | union ∩ blue, no 🚨; gate blue=True; list yday_gainer; 🔵; ret5=+0.3; leftover $1294.27 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $315.41 | ▼ 09:30 equity $9,866.68 vs yday $10,172.08 (-305.40) | 09:30 open · cash $315.41 (unchanged overnight, no fees) · equity $9,866.68 vs prior close $10,172.08 (-305.40) because holdings re-marked: ANF×8 yday $145.75 → 09:30 $148.67 +23.36; SEDG×38 yday $33.51 → 09:30 $31.50 -76.38; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×81 yday $15.66 → 09:30 $14.32 -108.54; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; VYX×144 yday $9.18 → 09:30 $9.06 -17.28; TTMI×10 yday $124.73 → 09:30 $117.20 -75.30; NVRI×56 yday $22.47 → 09:30 $22.28 -10.64 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $315.41 | ▼ 09:30 equity $9,817.31 vs yday $9,855.96 (-38.65) | 09:30 open · cash $315.41 (unchanged overnight, no fees) · equity $9,817.31 vs prior close $9,855.96 (-38.65) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; SEDG×38 yday $31.27 → 09:30 $32.22 +36.10; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×81 yday $14.20 → 09:30 $15.05 +68.85; URBN×15 yday $81.09 → 09:30 $80.69 -6.00; VYX×144 yday $8.90 → 09:30 $8.40 -72.00; TTMI×10 yday $120.19 → 09:30 $119.79 -4.00; NVRI×56 yday $22.28 → 09:30 $22.28 +0.00 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $315.41 | ▼ 09:30 equity $9,661.86 vs yday $9,720.92 (-59.06) | 09:30 open · cash $315.41 (unchanged overnight, no fees) · equity $9,661.86 vs prior close $9,720.92 (-59.06) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; SEDG×38 yday $31.80 → 09:30 $31.87 +2.66; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×81 yday $14.80 → 09:30 $14.75 -4.05; URBN×15 yday $80.69 → 09:30 $79.12 -23.55; VYX×144 yday $8.27 → 09:30 $8.30 +4.32; TTMI×10 yday $116.94 → 09:30 $116.68 -2.60; NVRI×56 yday $22.28 → 09:30 $22.05 -12.88 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $142.00 | $2.03 | $-25.65 | $1,449.38 | ▼ -25.65 after sell → book $9,659.83; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 38 | $31.87 | $2.12 | $-76.81 | $2,658.31 | ▼ -76.81 after sell → book $9,657.70; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $3,677.32 | ▼ -178.21 after sell → book $9,655.67; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 81 | $14.75 | $2.26 | $-100.88 | $4,869.81 | ▼ -100.88 after sell → book $9,653.41; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 15 | $79.12 | $2.06 | $-57.79 | $6,054.56 | ▼ -57.79 after sell → book $9,651.36; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `VYX` | 144 | $8.30 | $2.46 | $-98.48 | $7,247.30 | ▼ -98.48 after sell → book $9,648.90; vs 09:30 mark -2.46 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TTMI` | 10 | $116.68 | $2.04 | $-107.96 | $8,412.06 | ▼ -107.96 after sell → book $9,646.86; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `NVRI` | 56 | $22.05 | $2.18 | $-63.70 | $9,644.68 | ▼ -63.70 after sell → book $9,644.68; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,644.68 | ▲ 09:30 equity $9,644.68 vs yday $9,644.68 (+0.00) | 09:30 open · cash $9,644.68 · no holdings · equity $9,644.68 vs prior close $9,644.68 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 24 | $49.76 | $2.06 | — | $8,448.38 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1205.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 29 | $41.31 | $2.08 | — | $7,248.32 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1205.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 368 | $3.27 | $4.75 | — | $6,040.21 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1205.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 156 | $7.70 | $2.46 | — | $4,836.55 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1205.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,701.07 | — | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1205.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 76 | $15.70 | $2.22 | — | $2,505.66 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1205.59 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $22.78 | $2.15 | — | $1,318.95 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1205.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 324 | $3.72 | $4.18 | — | $109.49 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1205.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $109.49 | ▲ 09:30 equity $10,028.63 vs yday $9,979.77 (+48.86) | 09:30 open · cash $109.49 (unchanged overnight, no fees) · equity $10,028.63 vs prior close $9,979.77 (+48.86) because holdings re-marked: ATRC×24 yday $52.59 → 09:30 $52.88 +6.96; HRMY×29 yday $42.86 → 09:30 $42.93 +2.03; CABA×368 yday $3.57 → 09:30 $3.63 +22.08; VSTM×156 yday $8.02 → 09:30 $8.03 +1.56; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×76 yday $15.54 → 09:30 $15.45 -6.84; MMED×52 yday $23.76 → 09:30 $23.88 +6.24; CTMX×324 yday $3.72 → 09:30 $3.73 +3.24 | — |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $94.84 | — | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $18.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `GPRO` | 10 | $1.78 | $0.21 | — | $76.83 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $18.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 5 | $3.57 | $0.19 | — | $58.79 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $18.25 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 5 | $3.07 | $0.17 | — | $43.27 | — | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $18.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |

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
| 2026-08-18 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 8.98 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 8.98 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 8.98 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 8.98 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 8.98 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ELMT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HOOD` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ERO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `FCX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SBSW` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SCCO` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HMY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `INSP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RZLT` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 3.93 < 1 share @ 80.97 |
| 2026-08-27 | `GGB` | cash | leftover split 3.93 < 1 share @ 4.42 |
| 2026-08-27 | `MT` | cash | leftover split 3.93 < 1 share @ 75.12 |
| 2026-08-27 | `MU` | cash | leftover split 3.93 < 1 share @ 925.74 |
| 2026-08-27 | `TX` | cash | leftover split 3.93 < 1 share @ 55.20 |
| 2026-08-27 | `ANET` | cash | leftover split 3.93 < 1 share @ 190.90 |
| 2026-08-27 | `ASML` | cash | leftover split 3.93 < 1 share @ 1746.33 |
| 2026-08-27 | `DLO` | cash | leftover split 3.93 < 1 share @ 15.60 |
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
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
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
| 2026-09-04 | `OSCR` | cash | leftover split 18.25 < 1 share @ 30.65 |
| 2026-09-04 | `DELL` | cash | leftover split 18.25 < 1 share @ 486.31 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 24 | 2026-09-03 @ $49.76 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1205.59 |
| `HRMY` | 29 | 2026-09-03 @ $41.31 | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1205.59 |
| `CABA` | 368 | 2026-09-03 @ $3.27 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1205.59 |
| `VSTM` | 156 | 2026-09-03 @ $7.70 | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1205.59 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ blue, no 🚨; gate blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1205.59 |
| `CRK` | 76 | 2026-09-03 @ $15.70 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1205.59 |
| `MMED` | 52 | 2026-09-03 @ $22.78 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1205.59 |
| `CTMX` | 324 | 2026-09-03 @ $3.72 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1205.59 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | union ∩ blue, no 🚨; gate blue=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $18.25 |
| `GPRO` | 10 | 2026-09-04 @ $1.78 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $18.25 |
| `EOSE` | 5 | 2026-09-04 @ $3.57 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $18.25 |
| `SLBT` | 5 | 2026-09-04 @ $3.07 | union ∩ blue, no 🚨; gate blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $18.25 |
