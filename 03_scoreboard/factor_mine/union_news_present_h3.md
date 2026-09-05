# Factor mine action — `union_news_present_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ news_present, no 🚨

Cash book **+2.81%** ($10,281) · signal-only (no cash/fees) was +23.08%. Starts YES **16/17**. Fills 89 · skips 144 · realized $+29.95.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `news_present=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $74.42.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | — | $560.20 | $10,051.46 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $560.20 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833 | $10,054.84 | +3.38 | DVN, TMC, TGB, DNN, NB | — | $240.19 | $10,058.88 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 |
| 2026-08-18 | -6.20 | $240.19 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | $9,876.26 | -182.62 | — | — | $240.19 | $9,561.28 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | 09:30 open · cash $240.19 (unchanged overnight, no fees) · equity $9,876.26 vs prior close $10,058.88 (-182.62) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; DAVE×3 yday $341.43 → 09:30 $330.53 -32.70; SLG×21 yday $56.11 → 09:30 $56.00 -2.31; MARA×138 yday $9.72 → 09:30 $9.36 -49.68; LDI×1334 yday $0.88 → 09:30 $0.87 -6.67; BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×17 yday $3.77 → 09:30 $3.72 -0.85; TGB×8 yday $8.77 → 09:30 $8.55 -1.76; DNN×21 yday $3.19 → 09:30 $3.11 -1.68; NB×13 yday $4.81 → 09:30 $4.66 -1.95 |
| 2026-08-19 | -7.20 | $240.19 | TLN×3, VST×8, NRG×10, DAVE×3, SLG×21, MARA×138, LDI×1334, BTBT×833, DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | $9,598.39 | +37.11 | — | TLN, VST, NRG, DAVE, SLG, MARA, LDI, BTBT | $9,247.48 | $9,556.52 | DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | 09:30 open · cash $240.19 (unchanged overnight, no fees) · equity $9,598.39 vs prior close $9,561.28 (+37.11) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; DAVE×3 yday $333.14 → 09:30 $334.00 +2.58; SLG×21 yday $56.84 → 09:30 $57.50 +13.86; MARA×138 yday $8.96 → 09:30 $8.91 -6.90; LDI×1334 yday $0.86 → 09:30 $0.88 +29.35; BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×17 yday $3.92 → 09:30 $3.93 +0.17; TGB×8 yday $8.36 → 09:30 $8.70 +2.72; DNN×21 yday $3.15 → 09:30 $3.19 +0.84; NB×13 yday $4.53 → 09:30 $4.60 +0.91 |
| 2026-08-20 | +1.12 | $9,247.48 | DVN×1, TMC×17, TGB×8, DNN×21, NB×13 | $9,554.99 | -1.53 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | DVN, TMC, TGB, DNN, NB | $74.01 | $9,753.61 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8 | 09:30 open · cash $9,247.48 (unchanged overnight, no fees) · equity $9,554.99 vs prior close $9,556.52 (-1.53) because holdings re-marked: DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×17 yday $3.97 → 09:30 $3.92 -0.85; TGB×8 yday $8.47 → 09:30 $8.35 -0.96; DNN×21 yday $3.22 → 09:30 $3.20 -0.42; NB×13 yday $4.46 → 09:30 $4.45 -0.13 |
| 2026-08-21 | +3.25 | $74.01 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8 | $10,011.72 | +258.11 | AUTL, CRDL, CYPH | — | $49.36 | $10,009.57 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, AUTL×3, CRDL×4, CYPH×7 | 09:30 open · cash $74.01 (unchanged overnight, no fees) · equity $10,011.72 vs prior close $9,753.61 (+258.11) because holdings re-marked: AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×206 yday $5.57 → 09:30 $5.67 +20.60; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×682 yday $1.75 → 09:30 $1.79 +27.28; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $49.36 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, AUTL×3, CRDL×4, CYPH×7 | $10,126.37 | +116.80 | — | — | $49.36 | $9,980.37 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, AUTL×3, CRDL×4, CYPH×7 | 09:30 open · cash $49.36 (unchanged overnight, no fees) · equity $10,126.37 vs prior close $10,009.57 (+116.80) because holdings re-marked: AG×58 yday $21.09 → 09:30 $21.47 +22.04; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×57 yday $20.97 → 09:30 $21.26 +16.53; HDSN×206 yday $5.63 → 09:30 $5.69 +12.36; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×40 yday $32.76 → 09:30 $33.21 +18.00; NFGC×682 yday $1.84 → 09:30 $1.86 +13.64; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUTL×3 yday $2.41 → 09:30 $2.36 -0.15; CRDL×4 yday $1.86 → 09:30 $1.87 +0.04; CYPH×7 yday $1.42 → 09:30 $1.83 +2.87 |
| 2026-08-25 | +1.80 | $49.36 | AG×58, BHP×13, CDE×57, HDSN×206, IAG×60, KGC×40, NFGC×682, WPM×8, AUTL×3, CRDL×4, CYPH×7 | $10,043.96 | +63.59 | MOS, OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $396.55 | $9,991.74 | AUTL×3, CRDL×4, CYPH×7, MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624 | 09:30 open · cash $49.36 (unchanged overnight, no fees) · equity $10,043.96 vs prior close $9,980.37 (+63.59) because holdings re-marked: AG×58 yday $20.57 → 09:30 $20.73 +9.28; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×57 yday $20.49 → 09:30 $20.85 +20.52; HDSN×206 yday $5.57 → 09:30 $5.53 -8.24; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×40 yday $32.47 → 09:30 $32.76 +11.60; NFGC×682 yday $1.90 → 09:30 $1.91 +6.82; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUTL×3 yday $2.38 → 09:30 $2.32 -0.18; CRDL×4 yday $1.80 → 09:30 $1.90 +0.40; CYPH×7 yday $1.64 → 09:30 $1.70 +0.42 |
| 2026-08-26 | +2.02 | $396.55 | AUTL×3, CRDL×4, CYPH×7, MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624 | $9,991.74 | +0.00 | — | — | $396.55 | $9,987.53 | AUTL×3, CRDL×4, CYPH×7, MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624 | 09:30 open · cash $396.55 (unchanged overnight, no fees) · equity $9,991.74 vs prior close $9,991.74 (+0.00) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.34 +0.00; CRDL×4 yday $1.90 → 09:30 $1.90 +0.00; CYPH×7 yday $1.64 → 09:30 $1.64 +0.00; MOS×52 yday $23.75 → 09:30 $23.75 +0.00; OCUL×114 yday $10.92 → 09:30 $10.92 +0.00; INSP×20 yday $61.47 → 09:30 $61.47 +0.00; CRMD×150 yday $8.28 → 09:30 $8.28 +0.00; RZLT×238 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×771 yday $1.61 → 09:30 $1.61 +0.00; NPWR×624 yday $2.02 → 09:30 $2.02 +0.00 |
| 2026-08-27 | — | $396.55 | AUTL×3, CRDL×4, CYPH×7, MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624 | $10,037.19 | +49.66 | RRC | AUTL, CRDL, CYPH | $381.61 | $9,903.45 | MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624, RRC×1 | 09:30 open · cash $396.55 (unchanged overnight, no fees) · equity $10,037.19 vs prior close $9,987.53 (+49.66) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.41 +0.21; CRDL×4 yday $1.90 → 09:30 $2.03 +0.52; CYPH×7 yday $1.64 → 09:30 $1.60 -0.28; MOS×52 yday $23.75 → 09:30 $24.84 +56.68; OCUL×114 yday $10.92 → 09:30 $10.79 -14.82; INSP×20 yday $61.47 → 09:30 $60.07 -28.00; CRMD×150 yday $8.28 → 09:30 $8.60 +48.00; RZLT×238 yday $5.29 → 09:30 $5.01 -66.64; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×771 yday $1.61 → 09:30 $1.75 +107.94; NPWR×624 yday $2.02 → 09:30 $1.93 -56.16 |
| 2026-08-28 | +0.75 | $381.61 | MOS×52, OCUL×114, INSP×20, CRMD×150, RZLT×238, HCA×2, BMEA×771, NPWR×624, RRC×1 | $9,937.71 | +34.26 | CRK, SLI, ANF, BHVN, BZ, CAPR | OCUL, INSP, CRMD, RZLT, HCA, BMEA, NPWR | $152.61 | $9,955.62 | MOS×52, RRC×1, CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | 09:30 open · cash $381.61 (unchanged overnight, no fees) · equity $9,937.71 vs prior close $9,903.45 (+34.26) because holdings re-marked: MOS×52 yday $24.16 → 09:30 $24.00 -8.32; OCUL×114 yday $10.77 → 09:30 $10.63 -15.96; INSP×20 yday $61.80 → 09:30 $62.10 +6.00; CRMD×150 yday $8.39 → 09:30 $8.49 +15.00; RZLT×238 yday $5.04 → 09:30 $5.07 +7.14; HCA×2 yday $427.16 → 09:30 $424.61 -5.10; BMEA×771 yday $1.71 → 09:30 $1.74 +23.13; NPWR×624 yday $1.81 → 09:30 $1.83 +12.48; RRC×1 yday $41.55 → 09:30 $41.44 -0.11 |
| 2026-08-31 | -5.85 | $152.61 | MOS×52, RRC×1, CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | $9,740.84 | -214.78 | — | MOS | $1,385.45 | $9,724.82 | RRC×1, CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | 09:30 open · cash $152.61 (unchanged overnight, no fees) · equity $9,740.84 vs prior close $9,955.62 (-214.78) because holdings re-marked: MOS×52 yday $23.76 → 09:30 $23.75 -0.52; RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×99 yday $14.62 → 09:30 $14.56 -5.94; SLI×552 yday $2.64 → 09:30 $2.51 -71.76; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×84 yday $16.12 → 09:30 $15.44 -57.12; BZ×77 yday $18.00 → 09:30 $17.89 -8.47; CAPR×156 yday $10.06 → 09:30 $9.44 -96.72 |
| 2026-09-01 | -6.30 | $1,385.45 | RRC×1, CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | $9,878.46 | +153.64 | — | RRC | $1,426.33 | $9,888.36 | CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | 09:30 open · cash $1,385.45 (unchanged overnight, no fees) · equity $9,878.46 vs prior close $9,724.82 (+153.64) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×99 yday $14.51 → 09:30 $14.31 -19.80; SLI×552 yday $2.51 → 09:30 $2.70 +104.88; ANF×9 yday $149.28 → 09:30 $142.47 -61.29; BHVN×84 yday $15.40 → 09:30 $15.45 +4.20; BZ×77 yday $17.90 → 09:30 $17.37 -40.81; CAPR×156 yday $9.36 → 09:30 $10.43 +166.92 |
| 2026-09-02 | -3.83 | $1,426.33 | CRK×99, SLI×552, ANF×9, BHVN×84, BZ×77, CAPR×156 | $10,048.56 | +160.20 | — | CRK, SLI, ANF, BHVN, BZ, CAPR | $10,029.97 | $10,029.97 | — | 09:30 open · cash $1,426.33 (unchanged overnight, no fees) · equity $10,048.56 vs prior close $9,888.36 (+160.20) because holdings re-marked: CRK×99 yday $14.90 → 09:30 $15.82 +91.08; SLI×552 yday $2.70 → 09:30 $2.67 -16.56; ANF×9 yday $143.00 → 09:30 $142.00 -9.00; BHVN×84 yday $15.45 → 09:30 $15.39 -5.04; BZ×77 yday $17.17 → 09:30 $17.29 +9.24; CAPR×156 yday $10.19 → 09:30 $10.77 +90.48 |
| 2026-09-03 | -0.90 | $10,029.97 | — | $10,029.97 | +0.00 | ATRC, HRMY, CABA, VSTM, RVTY, GPRO, FRVO, CRK | — | $137.72 | $10,769.21 | ATRC×25, HRMY×30, CABA×383, VSTM×162, RVTY×9, GPRO×1027, FRVO×68, CRK×79 | 09:30 open · cash $10,029.97 · no holdings · equity $10,029.97 vs prior close $10,029.97 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $137.72 | ATRC×25, HRMY×30, CABA×383, VSTM×162, RVTY×9, GPRO×1027, FRVO×68, CRK×79 | $10,921.79 | +152.58 | NVAX, BVS, BAK | — | $74.42 | $10,280.65 | ATRC×25, HRMY×30, CABA×383, VSTM×162, RVTY×9, GPRO×1027, FRVO×68, CRK×79, NVAX×2, BVS×1, BAK×14 | 09:30 open · cash $137.72 (unchanged overnight, no fees) · equity $10,921.79 vs prior close $10,769.21 (+152.58) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; CABA×383 yday $3.57 → 09:30 $3.63 +22.98; VSTM×162 yday $8.02 → 09:30 $8.03 +1.62; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×1027 yday $1.69 → 09:30 $1.78 +92.43; FRVO×68 yday $17.98 → 09:30 $18.27 +19.72; CRK×79 yday $15.54 → 09:30 $15.45 -7.11 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `TLN` | 3 | $359.83 | $2.00 | — | $8,918.51 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `VST` | 8 | $146.90 | $2.01 | — | $7,741.30 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+3.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NRG` | 10 | $120.00 | $2.02 | — | $6,539.28 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+0.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `DAVE` | 3 | $330.91 | $2.00 | — | $5,544.55 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-8.6; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `SLG` | 21 | $57.61 | $2.05 | — | $4,332.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+5.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🔴 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MARA` | 138 | $9.01 | $2.40 | — | $3,086.90 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-13.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `LDI` | 1334 | $0.94 | $16.50 | — | $1,820.44 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $560.20 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $560.20 | ▲ 09:30 equity $10,054.84 vs yday $10,051.46 (+3.38) | 09:30 open · cash $560.20 (unchanged overnight, no fees) · equity $10,054.84 vs prior close $10,051.46 (+3.38) because holdings re-marked: TLN×3 yday $362.74 → 09:30 $367.88 +15.42; VST×8 yday $148.13 → 09:30 $149.37 +9.92; NRG×10 yday $126.24 → 09:30 $127.40 +11.60; DAVE×3 yday $334.57 → 09:30 $336.94 +7.11; SLG×21 yday $56.09 → 09:30 $55.37 -15.12; MARA×138 yday $9.20 → 09:30 $9.22 +2.76; LDI×1334 yday $0.90 → 09:30 $0.91 +13.34; BTBT×833 yday $1.57 → 09:30 $1.52 -41.65 | — |
| 2026-08-17 09:30 ET | **BUY** | `DVN` | 1 | $46.18 | $0.46 | — | $513.55 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+6.7; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟢 vol🔴 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `TMC` | 17 | $4.05 | $0.74 | — | $443.96 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=-12.3; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `TGB` | 8 | $8.46 | $0.70 | — | $375.58 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.4; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `DNN` | 21 | $3.24 | $0.74 | — | $306.80 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+0.3; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NB` | 13 | $5.07 | $0.70 | — | $240.19 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=-4.7; leftover $70.02 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.19 | ▼ 09:30 equity $9,876.26 vs yday $10,058.88 (-182.62) | 09:30 open · cash $240.19 (unchanged overnight, no fees) · equity $9,876.26 vs prior close $10,058.88 (-182.62) because holdings re-marked: TLN×3 yday $356.92 → 09:30 $350.89 -18.09; VST×8 yday $146.11 → 09:30 $144.50 -12.88; NRG×10 yday $122.37 → 09:30 $121.92 -4.50; DAVE×3 yday $341.43 → 09:30 $330.53 -32.70; SLG×21 yday $56.11 → 09:30 $56.00 -2.31; MARA×138 yday $9.72 → 09:30 $9.36 -49.68; LDI×1334 yday $0.88 → 09:30 $0.87 -6.67; BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; DVN×1 yday $47.57 → 09:30 $48.00 +0.43; TMC×17 yday $3.77 → 09:30 $3.72 -0.85; TGB×8 yday $8.77 → 09:30 $8.55 -1.76; DNN×21 yday $3.19 → 09:30 $3.11 -1.68; NB×13 yday $4.81 → 09:30 $4.66 -1.95 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $240.19 | ▲ 09:30 equity $9,598.39 vs yday $9,561.28 (+37.11) | 09:30 open · cash $240.19 (unchanged overnight, no fees) · equity $9,598.39 vs prior close $9,561.28 (+37.11) because holdings re-marked: TLN×3 yday $317.66 → 09:30 $321.00 +10.02; VST×8 yday $140.52 → 09:30 $140.74 +1.76; NRG×10 yday $115.56 → 09:30 $116.20 +6.40; DAVE×3 yday $333.14 → 09:30 $334.00 +2.58; SLG×21 yday $56.84 → 09:30 $57.50 +13.86; MARA×138 yday $8.96 → 09:30 $8.91 -6.90; LDI×1334 yday $0.86 → 09:30 $0.88 +29.35; BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; DVN×1 yday $47.83 → 09:30 $48.22 +0.39; TMC×17 yday $3.92 → 09:30 $3.93 +0.17; TGB×8 yday $8.36 → 09:30 $8.70 +2.72; DNN×21 yday $3.15 → 09:30 $3.19 +0.84; NB×13 yday $4.53 → 09:30 $4.60 +0.91 | — |
| 2026-08-19 09:30 ET | **SELL** | `TLN` | 3 | $321.00 | $2.02 | $-120.51 | $1,201.17 | ▼ -120.51 after sell → book $9,596.37; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `VST` | 8 | $140.74 | $2.03 | $-53.33 | $2,325.06 | ▼ -53.33 after sell → book $9,594.34; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NRG` | 10 | $116.20 | $2.04 | $-42.06 | $3,485.02 | ▼ -42.06 after sell → book $9,592.30; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `DAVE` | 3 | $334.00 | $2.02 | $+5.25 | $4,485.00 | ▲ +5.25 after sell → book $9,590.28; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `SLG` | 21 | $57.50 | $2.07 | $-6.44 | $5,690.42 | ▼ -6.44 after sell → book $9,588.20; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MARA` | 138 | $8.91 | $2.44 | $-18.64 | $6,917.57 | ▼ -18.64 after sell → book $9,585.77; vs 09:30 mark -2.43 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `LDI` | 1334 | $0.88 | $15.97 | $-108.51 | $8,075.51 | ▼ -108.51 after sell → book $9,569.79; vs 09:30 mark -15.98 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $9,247.48 | ▼ -88.28 after sell → book $9,558.90; vs 09:30 mark -10.89 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,247.48 | ▼ 09:30 equity $9,554.99 vs yday $9,556.52 (-1.53) | 09:30 open · cash $9,247.48 (unchanged overnight, no fees) · equity $9,554.99 vs prior close $9,556.52 (-1.53) because holdings re-marked: DVN×1 yday $48.19 → 09:30 $49.02 +0.83; TMC×17 yday $3.97 → 09:30 $3.92 -0.85; TGB×8 yday $8.47 → 09:30 $8.35 -0.96; DNN×21 yday $3.22 → 09:30 $3.20 -0.42; NB×13 yday $4.46 → 09:30 $4.45 -0.13 | — |
| 2026-08-20 09:30 ET | **SELL** | `DVN` | 1 | $49.02 | $0.51 | $+1.86 | $9,295.99 | ▲ +1.86 after sell → book $9,554.48; vs 09:30 mark -0.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TMC` | 17 | $3.92 | $0.74 | $-3.69 | $9,361.89 | ▼ -3.69 after sell → book $9,553.74; vs 09:30 mark -0.74 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `TGB` | 8 | $8.35 | $0.71 | $-2.29 | $9,427.98 | ▼ -2.29 after sell → book $9,553.03; vs 09:30 mark -0.71 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `DNN` | 21 | $3.20 | $0.76 | $-2.34 | $9,494.42 | ▼ -2.34 after sell → book $9,552.27; vs 09:30 mark -0.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `NB` | 13 | $4.45 | $0.64 | $-9.40 | $9,551.64 | ▼ -9.40 after sell → book $9,551.64; vs 09:30 mark -0.63 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 58 | $20.55 | $2.16 | — | $8,357.57 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 13 | $91.01 | $2.03 | — | $7,172.41 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $5,993.20 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 206 | $5.77 | $2.66 | — | $4,801.92 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 60 | $19.63 | $2.17 | — | $3,621.95 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 40 | $29.63 | $2.11 | — | $2,434.64 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 682 | $1.75 | $8.80 | — | $1,232.35 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $74.01 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1193.95 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $74.01 | ▲ 09:30 equity $10,011.72 vs yday $9,753.61 (+258.11) | 09:30 open · cash $74.01 (unchanged overnight, no fees) · equity $10,011.72 vs prior close $9,753.61 (+258.11) because holdings re-marked: AG×58 yday $21.19 → 09:30 $21.90 +41.18; BHP×13 yday $93.63 → 09:30 $95.72 +27.17; CDE×57 yday $21.11 → 09:30 $21.75 +36.48; HDSN×206 yday $5.57 → 09:30 $5.67 +20.60; IAG×60 yday $20.50 → 09:30 $21.17 +40.20; KGC×40 yday $31.43 → 09:30 $32.17 +29.60; NFGC×682 yday $1.75 → 09:30 $1.79 +27.28; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 3 | $2.47 | $0.08 | — | $66.52 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 4 | $1.93 | $0.09 | — | $58.71 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 7 | $1.32 | $0.11 | — | $49.36 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $9.25 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.36 | ▲ 09:30 equity $10,126.37 vs yday $10,009.57 (+116.80) | 09:30 open · cash $49.36 (unchanged overnight, no fees) · equity $10,126.37 vs prior close $10,009.57 (+116.80) because holdings re-marked: AG×58 yday $21.09 → 09:30 $21.47 +22.04; BHP×13 yday $97.03 → 09:30 $97.34 +4.03; CDE×57 yday $20.97 → 09:30 $21.26 +16.53; HDSN×206 yday $5.63 → 09:30 $5.69 +12.36; IAG×60 yday $21.14 → 09:30 $21.44 +18.00; KGC×40 yday $32.76 → 09:30 $33.21 +18.00; NFGC×682 yday $1.84 → 09:30 $1.86 +13.64; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUTL×3 yday $2.41 → 09:30 $2.36 -0.15; CRDL×4 yday $1.86 → 09:30 $1.87 +0.04; CYPH×7 yday $1.42 → 09:30 $1.83 +2.87 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $49.36 | ▲ 09:30 equity $10,043.96 vs yday $9,980.37 (+63.59) | 09:30 open · cash $49.36 (unchanged overnight, no fees) · equity $10,043.96 vs prior close $9,980.37 (+63.59) because holdings re-marked: AG×58 yday $20.57 → 09:30 $20.73 +9.28; BHP×13 yday $96.66 → 09:30 $95.95 -9.23; CDE×57 yday $20.49 → 09:30 $20.85 +20.52; HDSN×206 yday $5.57 → 09:30 $5.53 -8.24; IAG×60 yday $21.36 → 09:30 $21.63 +16.20; KGC×40 yday $32.47 → 09:30 $32.76 +11.60; NFGC×682 yday $1.90 → 09:30 $1.91 +6.82; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUTL×3 yday $2.38 → 09:30 $2.32 -0.18; CRDL×4 yday $1.80 → 09:30 $1.90 +0.40; CYPH×7 yday $1.64 → 09:30 $1.70 +0.42 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 58 | $20.73 | $2.18 | $+6.09 | $1,249.51 | ▲ +6.09 after sell → book $10,041.77; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 13 | $95.95 | $2.05 | $+60.14 | $2,494.81 | ▲ +60.14 after sell → book $10,039.72; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.85 | $2.18 | $+7.06 | $3,681.08 | ▲ +7.06 after sell → book $10,037.54; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 206 | $5.53 | $2.70 | $-54.80 | $4,817.56 | ▼ -54.80 after sell → book $10,034.84; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 60 | $21.63 | $2.19 | $+115.64 | $6,113.17 | ▲ +115.64 after sell → book $10,032.65; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 40 | $32.76 | $2.13 | $+120.96 | $7,421.44 | ▲ +120.96 after sell → book $10,030.52; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 682 | $1.91 | $8.92 | $+91.40 | $8,715.14 | ▲ +91.40 after sell → book $10,021.60; vs 09:30 mark -8.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $9,993.10 | ▲ +119.63 after sell → book $10,019.56; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `MOS` | 52 | $24.00 | $2.15 | — | $8,742.96 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ⚪; ret5=+13.0; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟡 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `OCUL` | 114 | $10.92 | $2.33 | — | $7,495.75 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+10.4; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `INSP` | 20 | $61.47 | $2.05 | — | $6,264.30 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ret5=+9.2; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟢 |
| 2026-08-25 09:30 ET | **BUY** | `CRMD` | 150 | $8.28 | $2.44 | — | $5,019.86 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+8.8; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `RZLT` | 238 | $5.23 | $3.07 | — | $3,772.05 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+10.7; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `HCA` | 2 | $429.24 | $2.00 | — | $2,911.57 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+6.1; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🔴 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 771 | $1.62 | $9.95 | — | $1,652.60 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1249.14 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 624 | $2.00 | $8.05 | — | $396.55 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1249.14 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $396.55 | ▲ 09:30 equity $9,991.74 vs yday $9,991.74 (+0.00) | 09:30 open · cash $396.55 (unchanged overnight, no fees) · equity $9,991.74 vs prior close $9,991.74 (+0.00) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.34 +0.00; CRDL×4 yday $1.90 → 09:30 $1.90 +0.00; CYPH×7 yday $1.64 → 09:30 $1.64 +0.00; MOS×52 yday $23.75 → 09:30 $23.75 +0.00; OCUL×114 yday $10.92 → 09:30 $10.92 +0.00; INSP×20 yday $61.47 → 09:30 $61.47 +0.00; CRMD×150 yday $8.28 → 09:30 $8.28 +0.00; RZLT×238 yday $5.29 → 09:30 $5.29 +0.00; HCA×2 yday $428.50 → 09:30 $428.50 +0.00; BMEA×771 yday $1.61 → 09:30 $1.61 +0.00; NPWR×624 yday $2.02 → 09:30 $2.02 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $396.55 | ▲ 09:30 equity $10,037.19 vs yday $9,987.53 (+49.66) | 09:30 open · cash $396.55 (unchanged overnight, no fees) · equity $10,037.19 vs prior close $9,987.53 (+49.66) because holdings re-marked: AUTL×3 yday $2.34 → 09:30 $2.41 +0.21; CRDL×4 yday $1.90 → 09:30 $2.03 +0.52; CYPH×7 yday $1.64 → 09:30 $1.60 -0.28; MOS×52 yday $23.75 → 09:30 $24.84 +56.68; OCUL×114 yday $10.92 → 09:30 $10.79 -14.82; INSP×20 yday $61.47 → 09:30 $60.07 -28.00; CRMD×150 yday $8.28 → 09:30 $8.60 +48.00; RZLT×238 yday $5.29 → 09:30 $5.01 -66.64; HCA×2 yday $428.50 → 09:30 $427.50 -2.00; BMEA×771 yday $1.61 → 09:30 $1.75 +107.94; NPWR×624 yday $2.02 → 09:30 $1.93 -56.16 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 3 | $2.41 | $0.10 | $-0.36 | $403.68 | ▼ -0.36 after sell → book $10,037.09; vs 09:30 mark -0.10 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 4 | $2.03 | $0.11 | $+0.20 | $411.69 | ▲ +0.20 after sell → book $10,036.98; vs 09:30 mark -0.11 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 7 | $1.60 | $0.15 | $+1.69 | $422.74 | ▲ +1.69 after sell → book $10,036.83; vs 09:30 mark -0.15 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **BUY** | `RRC` | 1 | $40.72 | $0.41 | — | $381.61 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.8; leftover $70.46 | join🟢 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $381.61 | ▲ 09:30 equity $9,937.71 vs yday $9,903.45 (+34.26) | 09:30 open · cash $381.61 (unchanged overnight, no fees) · equity $9,937.71 vs prior close $9,903.45 (+34.26) because holdings re-marked: MOS×52 yday $24.16 → 09:30 $24.00 -8.32; OCUL×114 yday $10.77 → 09:30 $10.63 -15.96; INSP×20 yday $61.80 → 09:30 $62.10 +6.00; CRMD×150 yday $8.39 → 09:30 $8.49 +15.00; RZLT×238 yday $5.04 → 09:30 $5.07 +7.14; HCA×2 yday $427.16 → 09:30 $424.61 -5.10; BMEA×771 yday $1.71 → 09:30 $1.74 +23.13; NPWR×624 yday $1.81 → 09:30 $1.83 +12.48; RRC×1 yday $41.55 → 09:30 $41.44 -0.11 | — |
| 2026-08-28 09:30 ET | **SELL** | `OCUL` | 114 | $10.63 | $2.36 | $-37.75 | $1,591.07 | ▼ -37.75 after sell → book $9,935.35; vs 09:30 mark -2.36 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `INSP` | 20 | $62.10 | $2.07 | $+8.48 | $2,831.00 | ▲ +8.48 after sell → book $9,933.28; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRMD` | 150 | $8.49 | $2.48 | $+26.58 | $4,102.02 | ▲ +26.58 after sell → book $9,930.80; vs 09:30 mark -2.48 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `RZLT` | 238 | $5.07 | $3.12 | $-44.27 | $5,305.56 | ▼ -44.27 after sell → book $9,927.68; vs 09:30 mark -3.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `HCA` | 2 | $424.61 | $2.02 | $-13.27 | $6,152.77 | ▼ -13.27 after sell → book $9,925.67; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 771 | $1.74 | $10.08 | $+72.49 | $7,484.22 | ▲ +72.49 after sell → book $9,915.58; vs 09:30 mark -10.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 624 | $1.83 | $8.16 | $-122.29 | $8,617.98 | ▼ -122.29 after sell → book $9,907.42; vs 09:30 mark -8.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `CRK` | 99 | $14.42 | $2.29 | — | $7,188.11 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+1.1; leftover $1436.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SLI` | 552 | $2.60 | $7.12 | — | $5,745.79 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; ret5=+4.2; leftover $1436.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $4,441.47 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1436.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 84 | $16.95 | $2.24 | — | $3,015.43 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1436.33 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 77 | $18.50 | $2.22 | — | $1,588.71 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1436.33 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 156 | $9.19 | $2.46 | — | $152.61 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1436.33 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $152.61 | ▼ 09:30 equity $9,740.84 vs yday $9,955.62 (-214.78) | 09:30 open · cash $152.61 (unchanged overnight, no fees) · equity $9,740.84 vs prior close $9,955.62 (-214.78) because holdings re-marked: MOS×52 yday $23.76 → 09:30 $23.75 -0.52; RRC×1 yday $41.64 → 09:30 $41.11 -0.53; CRK×99 yday $14.62 → 09:30 $14.56 -5.94; SLI×552 yday $2.64 → 09:30 $2.51 -71.76; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×84 yday $16.12 → 09:30 $15.44 -57.12; BZ×77 yday $18.00 → 09:30 $17.89 -8.47; CAPR×156 yday $10.06 → 09:30 $9.44 -96.72 | — |
| 2026-08-31 09:30 ET | **SELL** | `MOS` | 52 | $23.75 | $2.17 | $-17.31 | $1,385.45 | ▼ -17.31 after sell → book $9,738.68; vs 09:30 mark -2.16 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,385.45 | ▲ 09:30 equity $9,878.46 vs yday $9,724.82 (+153.64) | 09:30 open · cash $1,385.45 (unchanged overnight, no fees) · equity $9,878.46 vs prior close $9,724.82 (+153.64) because holdings re-marked: RRC×1 yday $41.78 → 09:30 $41.32 -0.46; CRK×99 yday $14.51 → 09:30 $14.31 -19.80; SLI×552 yday $2.51 → 09:30 $2.70 +104.88; ANF×9 yday $149.28 → 09:30 $142.47 -61.29; BHVN×84 yday $15.40 → 09:30 $15.45 +4.20; BZ×77 yday $17.90 → 09:30 $17.37 -40.81; CAPR×156 yday $9.36 → 09:30 $10.43 +166.92 | — |
| 2026-09-01 09:30 ET | **SELL** | `RRC` | 1 | $41.32 | $0.44 | $-0.25 | $1,426.33 | ▼ -0.25 after sell → book $9,878.02; vs 09:30 mark -0.44 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,426.33 | ▲ 09:30 equity $10,048.56 vs yday $9,888.36 (+160.20) | 09:30 open · cash $1,426.33 (unchanged overnight, no fees) · equity $10,048.56 vs prior close $9,888.36 (+160.20) because holdings re-marked: CRK×99 yday $14.90 → 09:30 $15.82 +91.08; SLI×552 yday $2.70 → 09:30 $2.67 -16.56; ANF×9 yday $143.00 → 09:30 $142.00 -9.00; BHVN×84 yday $15.45 → 09:30 $15.39 -5.04; BZ×77 yday $17.17 → 09:30 $17.29 +9.24; CAPR×156 yday $10.19 → 09:30 $10.77 +90.48 | — |
| 2026-09-02 09:30 ET | **SELL** | `CRK` | 99 | $15.82 | $2.32 | $+134.00 | $2,990.19 | ▲ +134.00 after sell → book $10,046.24; vs 09:30 mark -2.32 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SLI` | 552 | $2.67 | $7.22 | $+24.29 | $4,456.81 | ▲ +24.29 after sell → book $10,039.02; vs 09:30 mark -7.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 9 | $142.00 | $2.04 | $-28.35 | $5,732.77 | ▼ -28.35 after sell → book $10,036.98; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 84 | $15.39 | $2.27 | $-135.55 | $7,023.27 | ▼ -135.55 after sell → book $10,034.72; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 77 | $17.29 | $2.24 | $-97.64 | $8,352.35 | ▼ -97.64 after sell → book $10,032.47; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `CAPR` | 156 | $10.77 | $2.50 | $+241.52 | $10,029.97 | ▲ +241.52 after sell → book $10,029.97; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,029.97 | ▲ 09:30 equity $10,029.97 vs yday $10,029.97 (+0.00) | 09:30 open · cash $10,029.97 · no holdings · equity $10,029.97 vs prior close $10,029.97 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `ATRC` | 25 | $49.76 | $2.06 | — | $8,783.91 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `HRMY` | 30 | $41.31 | $2.08 | — | $7,542.53 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `CABA` | 383 | $3.27 | $4.94 | — | $6,285.18 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `VSTM` | 162 | $7.70 | $2.48 | — | $5,035.30 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $3,899.83 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1253.75 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1027 | $1.22 | $13.25 | — | $2,633.64 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1253.75 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 68 | $18.40 | $2.19 | — | $1,380.24 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1253.75 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 79 | $15.70 | $2.23 | — | $137.72 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1253.75 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $137.72 | ▲ 09:30 equity $10,921.79 vs yday $10,769.21 (+152.58) | 09:30 open · cash $137.72 (unchanged overnight, no fees) · equity $10,921.79 vs prior close $10,769.21 (+152.58) because holdings re-marked: ATRC×25 yday $52.59 → 09:30 $52.88 +7.25; HRMY×30 yday $42.86 → 09:30 $42.93 +2.10; CABA×383 yday $3.57 → 09:30 $3.63 +22.98; VSTM×162 yday $8.02 → 09:30 $8.03 +1.62; RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×1027 yday $1.69 → 09:30 $1.78 +92.43; FRVO×68 yday $17.98 → 09:30 $18.27 +19.72; CRK×79 yday $15.54 → 09:30 $15.45 -7.11 | — |
| 2026-09-04 09:30 ET | **BUY** | `NVAX` | 2 | $10.41 | $0.21 | — | $116.68 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $27.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BVS` | 1 | $14.50 | $0.15 | — | $102.03 | — | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $27.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 14 | $1.95 | $0.32 | — | $74.42 | — | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $27.54 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

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
| 2026-08-17 | `ELF` | cash | leftover split 70.02 < 1 share @ 90.54 |
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
| 2026-08-18 | `DNN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OXY` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `APA` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MUR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `MLYS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TRMD` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OBE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CYPH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `DVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `TGB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `DNN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `STE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DHR` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SYK` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MLYS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 9.25 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 9.25 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 9.25 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 9.25 < 1 share @ 11.13 |
| 2026-08-21 | `CRSP` | cash | leftover split 9.25 < 1 share @ 59.72 |
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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `MOS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INSP` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `HCA` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-27 | `MOS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `OCUL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `INSP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CRMD` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RZLT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `HCA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ACMR` | cash | leftover split 70.46 < 1 share @ 80.97 |
| 2026-08-27 | `MU` | cash | leftover split 70.46 < 1 share @ 925.74 |
| 2026-08-27 | `ASML` | cash | leftover split 70.46 < 1 share @ 1746.33 |
| 2026-08-27 | `LRCX` | cash | leftover split 70.46 < 1 share @ 314.61 |
| 2026-08-27 | `NVDA` | cash | leftover split 70.46 < 1 share @ 212.64 |
| 2026-08-31 | `RRC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SLI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `RES` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PBF` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `NOV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `WTTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `CRK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SLI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `DK` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `BTE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `MTDR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `RES` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `OIS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `FTI` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `KMI` | hard_red | hard-red S=-6.30 sit; no new buys |
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
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ASND` | cash | leftover split 27.54 < 1 share @ 266.94 |
| 2026-09-04 | `OSCR` | cash | leftover split 27.54 < 1 share @ 30.65 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `ATRC` | 25 | 2026-09-03 @ $49.76 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+10.6; leftover $1253.75 |
| `HRMY` | 30 | 2026-09-03 @ $41.31 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+0.1; leftover $1253.75 |
| `CABA` | 383 | 2026-09-03 @ $3.27 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1253.75 |
| `VSTM` | 162 | 2026-09-03 @ $7.70 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.7; leftover $1253.75 |
| `RVTY` | 9 | 2026-09-03 @ $125.94 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1253.75 |
| `GPRO` | 1027 | 2026-09-03 @ $1.22 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1253.75 |
| `FRVO` | 68 | 2026-09-03 @ $18.40 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1253.75 |
| `CRK` | 79 | 2026-09-03 @ $15.70 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1253.75 |
| `NVAX` | 2 | 2026-09-04 @ $10.41 | union ∩ news_present, no 🚨; gate news_present=True; list flatten,ohlc_hot; ⚪; ret5=+11.1; leftover $27.54 |
| `BVS` | 1 | 2026-09-04 @ $14.50 | union ∩ news_present, no 🚨; gate news_present=True; list flatten; 🔵; ⚪; ret5=+0.8; leftover $27.54 |
| `BAK` | 14 | 2026-09-04 @ $1.95 | union ∩ news_present, no 🚨; gate news_present=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $27.54 |
