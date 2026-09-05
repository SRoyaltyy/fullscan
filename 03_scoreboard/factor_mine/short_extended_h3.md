# Factor mine action — `short_extended_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **short** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · ret_5>15

Cash book **-8.24%** ($9,176) · signal-only (no cash/fees) was -11.79%. Starts YES **0/17**. Fills 95 · skips 133 · realized $-652.35.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `ret_5_min=15.0` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $20,514.50.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | TNDM | — | $14,989.65 | $10,039.83 | TNDM×214 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $14,989.65 | TNDM×214 | $10,084.77 | +44.94 | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | — | $19,931.00 | $10,238.94 | TNDM×214, ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 | 09:30 open · cash $14,989.65 (unchanged overnight, no fees) · equity $10,084.77 vs prior close $10,039.83 (+44.94) because holdings re-marked: TNDM×214 yday $23.13 → 09:30 $22.92 +44.94 |
| 2026-08-17 | +2.25 | $19,931.00 | TNDM×214, ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127 | $10,294.30 | +55.36 | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | — | $24,976.88 | $10,373.14 | TNDM×214, ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127, CAPR×93, HTFL×15, UMAC×19, NPWR×335, LPTH×43, NMAX×58, ALOY×43, INO×601 | 09:30 open · cash $19,931.00 (unchanged overnight, no fees) · equity $10,294.30 vs prior close $10,238.94 (+55.36) because holdings re-marked: TNDM×214 yday $22.72 → 09:30 $22.50 +47.08; ARX×32 yday $19.58 → 09:30 $19.57 +0.32; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; AIRO×56 yday $9.57 → 09:30 $9.57 +0.00; MXCT×453 yday $1.32 → 09:30 $1.32 +0.00; QMLS×86 yday $7.32 → 09:30 $7.24 +6.88; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; TBBB×12 yday $47.79 → 09:30 $47.39 +4.80; AMPY×127 yday $4.78 → 09:30 $4.86 -10.16 |
| 2026-08-18 | -6.20 | $24,976.88 | TNDM×214, ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127, CAPR×93, HTFL×15, UMAC×19, NPWR×335, LPTH×43, NMAX×58, ALOY×43, INO×601 | $10,561.20 | +188.06 | — | TNDM | $20,231.88 | $10,434.83 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127, CAPR×93, HTFL×15, UMAC×19, NPWR×335, LPTH×43, NMAX×58, ALOY×43, INO×601 | 09:30 open · cash $24,976.88 (unchanged overnight, no fees) · equity $10,561.20 vs prior close $10,373.14 (+188.06) because holdings re-marked: TNDM×214 yday $22.25 → 09:30 $22.16 +20.33; ARX×32 yday $19.54 → 09:30 $19.57 -0.96; OMER×36 yday $17.36 → 09:30 $17.03 +11.88; AIRO×56 yday $9.41 → 09:30 $9.01 +22.40; MXCT×453 yday $1.32 → 09:30 $1.30 +9.06; QMLS×86 yday $7.14 → 09:30 $6.85 +24.94; AVAH×52 yday $12.69 → 09:30 $12.68 +0.52; TBBB×12 yday $48.45 → 09:30 $48.60 -1.80; AMPY×127 yday $4.82 → 09:30 $4.91 -11.43; CAPR×93 yday $7.45 → 09:30 $7.50 -4.65; HTFL×15 yday $41.94 → 09:30 $41.50 +6.60; UMAC×19 yday $30.15 → 09:30 $28.59 +29.64; NPWR×335 yday $1.73 → 09:30 $1.70 +10.05; LPTH×43 yday $14.80 → 09:30 $14.01 +33.97; NMAX×58 yday $10.36 → 09:30 $10.31 +2.90; ALOY×43 yday $13.86 → 09:30 $13.19 +28.60; INO×601 yday $1.15 → 09:30 $1.14 +6.01 |
| 2026-08-19 | -7.20 | $20,231.88 | ARX×32, OMER×36, AIRO×56, MXCT×453, QMLS×86, AVAH×52, TBBB×12, AMPY×127, CAPR×93, HTFL×15, UMAC×19, NPWR×335, LPTH×43, NMAX×58, ALOY×43, INO×601 | $10,346.15 | -88.68 | — | ARX, OMER, AIRO, MXCT, QMLS, AVAH, TBBB, AMPY | $15,419.01 | $10,375.44 | CAPR×93, HTFL×15, UMAC×19, NPWR×335, LPTH×43, NMAX×58, ALOY×43, INO×601 | 09:30 open · cash $20,231.88 (unchanged overnight, no fees) · equity $10,346.15 vs prior close $10,434.83 (-88.68) because holdings re-marked: ARX×32 yday $19.56 → 09:30 $19.58 -0.64; OMER×36 yday $17.19 → 09:30 $17.13 +2.16; AIRO×56 yday $8.98 → 09:30 $9.10 -6.72; MXCT×453 yday $1.27 → 09:30 $1.29 -9.06; QMLS×86 yday $6.74 → 09:30 $6.74 +0.00; AVAH×52 yday $12.67 → 09:30 $12.92 -13.00; TBBB×12 yday $48.06 → 09:30 $48.62 -6.72; AMPY×127 yday $4.82 → 09:30 $4.88 -7.62; CAPR×93 yday $7.08 → 09:30 $7.19 -10.23; HTFL×15 yday $45.23 → 09:30 $46.02 -11.85; UMAC×19 yday $30.58 → 09:30 $30.10 +9.12; NPWR×335 yday $1.65 → 09:30 $1.70 -16.75; LPTH×43 yday $14.22 → 09:30 $14.30 -3.44; NMAX×58 yday $11.43 → 09:30 $11.50 -4.06; ALOY×43 yday $13.50 → 09:30 $13.45 +2.15; INO×601 yday $1.20 → 09:30 $1.22 -12.02 |
| 2026-08-20 | +1.12 | $15,419.01 | CAPR×93, HTFL×15, UMAC×19, NPWR×335, LPTH×43, NMAX×58, ALOY×43, INO×601 | $10,436.28 | +60.84 | MRNA, AZI, CYPH, BNTX, BTGO, ASST, PPC, ABCL | CAPR, HTFL, UMAC, NPWR, LPTH, NMAX, ALOY, INO | $15,410.74 | $10,383.71 | MRNA×4, AZI×474, CYPH×565, BNTX×5, BTGO×98, ASST×40, PPC×21, ABCL×55 | 09:30 open · cash $15,419.01 (unchanged overnight, no fees) · equity $10,436.28 vs prior close $10,375.44 (+60.84) because holdings re-marked: CAPR×93 yday $7.98 → 09:30 $7.66 +29.76; HTFL×15 yday $45.53 → 09:30 $45.90 -5.55; UMAC×19 yday $28.30 → 09:30 $28.32 -0.38; NPWR×335 yday $1.67 → 09:30 $1.64 +10.05; LPTH×43 yday $13.24 → 09:30 $13.09 +6.45; NMAX×58 yday $10.91 → 09:30 $10.89 +1.16; ALOY×43 yday $12.51 → 09:30 $12.06 +19.35; INO×601 yday $1.30 → 09:30 $1.30 +0.00 |
| 2026-08-21 | +3.25 | $15,410.74 | MRNA×4, AZI×474, CYPH×565, BNTX×5, BTGO×98, ASST×40, PPC×21, ABCL×55 | $10,208.28 | -175.43 | AU, AEM, ARCT, INDP, CAN, DFDV, TEM | — | $20,372.94 | $9,706.10 | MRNA×4, AZI×474, CYPH×565, BNTX×5, BTGO×98, ASST×40, PPC×21, ABCL×55, AU×6, AEM×3, ARCT×65, INDP×524, CAN×2480, DFDV×180, TEM×11 | 09:30 open · cash $15,410.74 (unchanged overnight, no fees) · equity $10,208.28 vs prior close $10,383.71 (-175.43) because holdings re-marked: MRNA×4 yday $133.32 → 09:30 $133.11 +0.84; AZI×474 yday $1.44 → 09:30 $1.46 -9.48; CYPH×565 yday $1.19 → 09:30 $1.32 -73.45; BNTX×5 yday $110.89 → 09:30 $110.92 -0.15; BTGO×98 yday $6.60 → 09:30 $6.95 -34.30; ASST×40 yday $16.13 → 09:30 $17.66 -61.20; PPC×21 yday $31.24 → 09:30 $31.13 +2.31; ABCL×55 yday $11.57 → 09:30 $11.57 +0.00 |
| 2026-08-24 | -5.17 | $20,372.94 | MRNA×4, AZI×474, CYPH×565, BNTX×5, BTGO×98, ASST×40, PPC×21, ABCL×55, AU×6, AEM×3, ARCT×65, INDP×524, CAN×2480, DFDV×180, TEM×11 | $9,450.17 | -255.93 | — | — | $20,372.94 | $9,671.54 | MRNA×4, AZI×474, CYPH×565, BNTX×5, BTGO×98, ASST×40, PPC×21, ABCL×55, AU×6, AEM×3, ARCT×65, INDP×524, CAN×2480, DFDV×180, TEM×11 | 09:30 open · cash $20,372.94 (unchanged overnight, no fees) · equity $9,450.17 vs prior close $9,706.10 (-255.93) because holdings re-marked: MRNA×4 yday $145.13 → 09:30 $142.70 +9.72; AZI×474 yday $1.45 → 09:30 $1.46 -4.74; CYPH×565 yday $1.42 → 09:30 $1.83 -231.65; BNTX×5 yday $116.57 → 09:30 $114.11 +12.30; BTGO×98 yday $6.84 → 09:30 $6.87 -2.94; ASST×40 yday $18.22 → 09:30 $18.76 -21.60; PPC×21 yday $32.25 → 09:30 $32.50 -5.25; ABCL×55 yday $11.32 → 09:30 $10.97 +19.25; AU×6 yday $121.22 → 09:30 $120.50 +4.32; AEM×3 yday $216.06 → 09:30 $217.03 -2.91; ARCT×65 yday $13.45 → 09:30 $13.26 +12.35; INDP×524 yday $1.29 → 09:30 $1.24 +26.20; CAN×2480 yday $0.35 → 09:30 $0.38 -62.00; DFDV×180 yday $3.94 → 09:30 $4.15 -37.80; TEM×11 yday $72.69 → 09:30 $70.07 +28.82 |
| 2026-08-25 | +1.80 | $20,372.94 | MRNA×4, AZI×474, CYPH×565, BNTX×5, BTGO×98, ASST×40, PPC×21, ABCL×55, AU×6, AEM×3, ARCT×65, INDP×524, CAN×2480, DFDV×180, TEM×11 | $9,566.91 | -104.63 | SUJA, FWDI, DEFT, GORO, BMNR, RUM | MRNA, AZI, BNTX, BTGO, PPC, ABCL | $21,395.41 | $9,663.03 | CYPH×565, ASST×40, AU×6, AEM×3, ARCT×65, INDP×524, CAN×2480, DFDV×180, TEM×11, SUJA×90, FWDI×132, DEFT×1243, GORO×225, BMNR×32, RUM×85 | 09:30 open · cash $20,372.94 (unchanged overnight, no fees) · equity $9,566.91 vs prior close $9,671.54 (-104.63) because holdings re-marked: MRNA×4 yday $139.27 → 09:30 $141.19 -7.68; AZI×474 yday $1.40 → 09:30 $1.33 +33.18; CYPH×565 yday $1.64 → 09:30 $1.70 -33.90; BNTX×5 yday $111.34 → 09:30 $113.13 -8.95; BTGO×98 yday $6.97 → 09:30 $6.89 +7.84; ASST×40 yday $19.82 → 09:30 $20.90 -43.20; PPC×21 yday $32.22 → 09:30 $31.76 +9.66; ABCL×55 yday $10.52 → 09:30 $10.77 -13.75; AU×6 yday $118.66 → 09:30 $119.46 -4.80; AEM×3 yday $214.08 → 09:30 $200.48 +40.80; ARCT×65 yday $13.76 → 09:30 $14.34 -37.70; INDP×524 yday $1.16 → 09:30 $1.18 -10.48; CAN×2480 yday $0.37 → 09:30 $0.38 -24.80; DFDV×180 yday $4.19 → 09:30 $4.29 -18.00; TEM×11 yday $67.10 → 09:30 $66.45 +7.15 |
| 2026-08-26 | +2.02 | $21,395.41 | CYPH×565, ASST×40, AU×6, AEM×3, ARCT×65, INDP×524, CAN×2480, DFDV×180, TEM×11, SUJA×90, FWDI×132, DEFT×1243, GORO×225, BMNR×32, RUM×85 | $9,663.03 | +0.00 | — | — | $21,395.41 | $9,526.23 | CYPH×565, ASST×40, AU×6, AEM×3, ARCT×65, INDP×524, CAN×2480, DFDV×180, TEM×11, SUJA×90, FWDI×132, DEFT×1243, GORO×225, BMNR×32, RUM×85 | 09:30 open · cash $21,395.41 (unchanged overnight, no fees) · equity $9,663.03 vs prior close $9,663.03 (+0.00) because holdings re-marked: CYPH×565 yday $1.64 → 09:30 $1.64 +0.00; ASST×40 yday $20.20 → 09:30 $20.20 +0.00; AU×6 yday $118.55 → 09:30 $118.55 +0.00; AEM×3 yday $215.40 → 09:30 $215.40 +0.00; ARCT×65 yday $14.21 → 09:30 $14.21 +0.00; INDP×524 yday $1.25 → 09:30 $1.25 +0.00; CAN×2480 yday $0.36 → 09:30 $0.36 +0.00; DFDV×180 yday $4.16 → 09:30 $4.16 +0.00; TEM×11 yday $66.98 → 09:30 $66.98 +0.00; SUJA×90 yday $8.54 → 09:30 $8.54 +0.00; FWDI×132 yday $5.86 → 09:30 $5.86 +0.00; DEFT×1243 yday $0.62 → 09:30 $0.62 +0.00; GORO×225 yday $3.56 → 09:30 $3.56 +0.00; BMNR×32 yday $24.21 → 09:30 $24.21 +0.00; RUM×85 yday $9.35 → 09:30 $9.35 +0.00 |
| 2026-08-27 | — | $21,395.41 | CYPH×565, ASST×40, AU×6, AEM×3, ARCT×65, INDP×524, CAN×2480, DFDV×180, TEM×11, SUJA×90, FWDI×132, DEFT×1243, GORO×225, BMNR×32, RUM×85 | $9,340.30 | -185.93 | — | CYPH, ASST, AU, AEM, ARCT, INDP, CAN, DFDV, TEM | $14,154.86 | $9,393.71 | SUJA×90, FWDI×132, DEFT×1243, GORO×225, BMNR×32, RUM×85 | 09:30 open · cash $21,395.41 (unchanged overnight, no fees) · equity $9,340.30 vs prior close $9,526.23 (-185.93) because holdings re-marked: CYPH×565 yday $1.64 → 09:30 $1.60 +22.60; ASST×40 yday $20.20 → 09:30 $20.72 -20.80; AU×6 yday $118.55 → 09:30 $119.80 -7.50; AEM×3 yday $215.40 → 09:30 $219.50 -12.30; ARCT×65 yday $14.21 → 09:30 $15.35 -74.10; INDP×524 yday $1.25 → 09:30 $1.09 +83.84; CAN×2480 yday $0.36 → 09:30 $0.40 -99.20; DFDV×180 yday $4.16 → 09:30 $4.35 -34.20; TEM×11 yday $66.98 → 09:30 $67.48 -5.50; SUJA×90 yday $8.54 → 09:30 $9.39 -76.50; FWDI×132 yday $5.86 → 09:30 $5.97 -14.52; DEFT×1243 yday $0.62 → 09:30 $0.60 +24.86; GORO×225 yday $3.56 → 09:30 $3.77 -47.25; BMNR×32 yday $24.21 → 09:30 $24.24 -0.96; RUM×85 yday $9.35 → 09:30 $10.07 -61.20 |
| 2026-08-28 | +0.75 | $14,154.86 | SUJA×90, FWDI×132, DEFT×1243, GORO×225, BMNR×32, RUM×85 | $9,273.46 | -120.25 | FIGR, XHG, ERO, TRLV, FUTU, TXG, WPM | SUJA, FWDI, GORO, BMNR, RUM | $14,478.02 | $9,227.96 | DEFT×1243, FIGR×17, XHG×162, ERO×16, TRLV×58, FUTU×5, TXG×10, WPM×4 | 09:30 open · cash $14,154.86 (unchanged overnight, no fees) · equity $9,273.46 vs prior close $9,393.71 (-120.25) because holdings re-marked: SUJA×90 yday $9.44 → 09:30 $9.41 +2.70; FWDI×132 yday $5.93 → 09:30 $6.39 -60.72; DEFT×1243 yday $0.59 → 09:30 $0.60 -12.43; GORO×225 yday $3.56 → 09:30 $3.59 -6.75; BMNR×32 yday $24.91 → 09:30 $25.91 -32.00; RUM×85 yday $9.38 → 09:30 $9.51 -11.05 |
| 2026-08-31 | -5.85 | $14,478.02 | DEFT×1243, FIGR×17, XHG×162, ERO×16, TRLV×58, FUTU×5, TXG×10, WPM×4 | $9,376.14 | +148.18 | — | DEFT | $13,695.92 | $9,340.85 | FIGR×17, XHG×162, ERO×16, TRLV×58, FUTU×5, TXG×10, WPM×4 | 09:30 open · cash $14,478.02 (unchanged overnight, no fees) · equity $9,376.14 vs prior close $9,227.96 (+148.18) because holdings re-marked: DEFT×1243 yday $0.65 → 09:30 $0.62 +37.29; FIGR×17 yday $38.02 → 09:30 $35.50 +42.84; XHG×162 yday $3.80 → 09:30 $3.44 +58.32; ERO×16 yday $39.82 → 09:30 $38.60 +19.52; TRLV×58 yday $11.03 → 09:30 $12.41 -80.04; FUTU×5 yday $124.57 → 09:30 $122.82 +8.75; TXG×10 yday $64.85 → 09:30 $60.90 +39.50; WPM×4 yday $157.99 → 09:30 $152.49 +22.00 |
| 2026-09-01 | -6.30 | $13,695.92 | FIGR×17, XHG×162, ERO×16, TRLV×58, FUTU×5, TXG×10, WPM×4 | $9,369.54 | +28.69 | — | — | $13,695.92 | $9,448.14 | FIGR×17, XHG×162, ERO×16, TRLV×58, FUTU×5, TXG×10, WPM×4 | 09:30 open · cash $13,695.92 (unchanged overnight, no fees) · equity $9,369.54 vs prior close $9,340.85 (+28.69) because holdings re-marked: FIGR×17 yday $36.41 → 09:30 $36.80 -6.63; XHG×162 yday $3.44 → 09:30 $3.52 -12.96; ERO×16 yday $38.49 → 09:30 $37.30 +19.04; TRLV×58 yday $12.41 → 09:30 $11.89 +30.16; FUTU×5 yday $124.04 → 09:30 $122.22 +9.10; TXG×10 yday $61.40 → 09:30 $62.99 -15.90; WPM×4 yday $152.25 → 09:30 $150.78 +5.88 |
| 2026-09-02 | -3.83 | $13,695.92 | FIGR×17, XHG×162, ERO×16, TRLV×58, FUTU×5, TXG×10, WPM×4 | $9,483.82 | +35.68 | — | FIGR, TXG, WPM | $11,885.14 | $9,482.08 | XHG×162, ERO×16, TRLV×58, FUTU×5 | 09:30 open · cash $13,695.92 (unchanged overnight, no fees) · equity $9,483.82 vs prior close $9,448.14 (+35.68) because holdings re-marked: FIGR×17 yday $35.70 → 09:30 $35.46 +4.08; XHG×162 yday $3.43 → 09:30 $3.48 -8.10; ERO×16 yday $36.01 → 09:30 $35.95 +0.96; TRLV×58 yday $11.89 → 09:30 $11.54 +20.30; FUTU×5 yday $120.88 → 09:30 $119.82 +5.30; TXG×10 yday $62.92 → 09:30 $61.79 +11.30; WPM×4 yday $146.46 → 09:30 $146.00 +1.84 |
| 2026-09-03 | -0.90 | $11,885.14 | XHG×162, ERO×16, TRLV×58, FUTU×5 | $9,456.34 | -25.74 | DEFT, MRNA, ARCT, ALEC, CAN | FUTU | $15,923.47 | $9,329.49 | XHG×162, ERO×16, TRLV×58, DEFT×1411, MRNA×6, ARCT×57, ALEC×393, CAN×3151 | 09:30 open · cash $11,885.14 (unchanged overnight, no fees) · equity $9,456.34 vs prior close $9,482.08 (-25.74) because holdings re-marked: XHG×162 yday $3.51 → 09:30 $3.57 -9.72; ERO×16 yday $34.82 → 09:30 $35.62 -12.80; TRLV×58 yday $11.74 → 09:30 $11.78 -2.32; FUTU×5 yday $119.28 → 09:30 $119.46 -0.90 |
| 2026-09-04 | — | $15,923.47 | XHG×162, ERO×16, TRLV×58, DEFT×1411, MRNA×6, ARCT×57, ALEC×393, CAN×3151 | $9,231.99 | -97.50 | HQ, OABI, BRR | — | $20,514.50 | $9,175.63 | XHG×162, ERO×16, TRLV×58, DEFT×1411, MRNA×6, ARCT×57, ALEC×393, CAN×3151, HQ×90, OABI×302, BRR×651 | 09:30 open · cash $15,923.47 (unchanged overnight, no fees) · equity $9,231.99 vs prior close $9,329.49 (-97.50) because holdings re-marked: XHG×162 yday $3.32 → 09:30 $3.38 -9.72; ERO×16 yday $34.76 → 09:30 $35.82 -16.96; TRLV×58 yday $11.69 → 09:30 $11.89 -11.60; DEFT×1411 yday $0.65 → 09:30 $0.65 +0.00; MRNA×6 yday $150.81 → 09:30 $145.95 +29.16; ARCT×57 yday $16.74 → 09:30 $16.77 -1.71; ALEC×393 yday $2.72 → 09:30 $2.70 +7.86; CAN×3151 yday $0.31 → 09:30 $0.34 -94.53 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-13 09:30 ET | **SHORT** | `TNDM` | 214 | $23.33 | $2.97 | — | $14,989.65 | — | ret_5>15; gate ret_5_min=15.0; list flatten; ⚪; ret5=+19.7; leftover $5000.00 | join🟢 sector🟢 gen🟢 judge🟢 |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,989.65 | ▲ 09:30 equity $10,084.77 vs yday $10,039.83 (+44.94) | 09:30 open · cash $14,989.65 (unchanged overnight, no fees) · equity $10,084.77 vs prior close $10,039.83 (+44.94) because holdings re-marked: TNDM×214 yday $23.13 → 09:30 $22.92 +44.94 | — |
| 2026-08-14 09:30 ET | **SHORT** | `ARX` | 32 | $19.57 | $2.12 | — | $15,613.76 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $630.30 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `OMER` | 36 | $17.35 | $2.14 | — | $16,236.23 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $630.30 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AIRO` | 56 | $11.12 | $2.20 | — | $16,856.75 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $630.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `MXCT` | 453 | $1.39 | $5.95 | — | $17,480.48 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $630.30 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `QMLS` | 86 | $7.29 | $2.29 | — | $18,105.13 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $630.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AVAH` | 52 | $11.91 | $2.18 | — | $18,722.27 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+21.3; leftover $630.30 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `TBBB` | 12 | $48.82 | $2.06 | — | $19,306.04 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+21.0; leftover $630.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **SHORT** | `AMPY` | 127 | $4.94 | $2.42 | — | $19,931.00 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.4; leftover $630.30 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19,931.00 | ▲ 09:30 equity $10,294.30 vs yday $10,238.94 (+55.36) | 09:30 open · cash $19,931.00 (unchanged overnight, no fees) · equity $10,294.30 vs prior close $10,238.94 (+55.36) because holdings re-marked: TNDM×214 yday $22.72 → 09:30 $22.50 +47.08; ARX×32 yday $19.58 → 09:30 $19.57 +0.32; OMER×36 yday $17.19 → 09:30 $17.17 +0.72; AIRO×56 yday $9.57 → 09:30 $9.57 +0.00; MXCT×453 yday $1.32 → 09:30 $1.32 +0.00; QMLS×86 yday $7.32 → 09:30 $7.24 +6.88; AVAH×52 yday $12.32 → 09:30 $12.21 +5.72; TBBB×12 yday $47.79 → 09:30 $47.39 +4.80; AMPY×127 yday $4.78 → 09:30 $4.86 -10.16 | — |
| 2026-08-17 09:30 ET | **SHORT** | `CAPR` | 93 | $6.87 | $2.31 | — | $20,567.60 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.6; leftover $643.39 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `HTFL` | 15 | $41.23 | $2.07 | — | $21,183.98 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+46.0; leftover $643.39 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `UMAC` | 19 | $32.55 | $2.08 | — | $21,800.35 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $643.39 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `NPWR` | 335 | $1.92 | $4.40 | — | $22,439.14 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $643.39 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `LPTH` | 43 | $14.94 | $2.16 | — | $23,079.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $643.39 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **SHORT** | `NMAX` | 58 | $10.97 | $2.20 | — | $23,713.46 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $643.39 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `ALOY` | 43 | $14.66 | $2.16 | — | $24,341.69 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.0; leftover $643.39 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **SHORT** | `INO` | 601 | $1.07 | $7.88 | — | $24,976.88 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+62.7; leftover $643.39 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $24,976.88 | ▲ 09:30 equity $10,561.20 vs yday $10,373.14 (+188.06) | 09:30 open · cash $24,976.88 (unchanged overnight, no fees) · equity $10,561.20 vs prior close $10,373.14 (+188.06) because holdings re-marked: TNDM×214 yday $22.25 → 09:30 $22.16 +20.33; ARX×32 yday $19.54 → 09:30 $19.57 -0.96; OMER×36 yday $17.36 → 09:30 $17.03 +11.88; AIRO×56 yday $9.41 → 09:30 $9.01 +22.40; MXCT×453 yday $1.32 → 09:30 $1.30 +9.06; QMLS×86 yday $7.14 → 09:30 $6.85 +24.94; AVAH×52 yday $12.69 → 09:30 $12.68 +0.52; TBBB×12 yday $48.45 → 09:30 $48.60 -1.80; AMPY×127 yday $4.82 → 09:30 $4.91 -11.43; CAPR×93 yday $7.45 → 09:30 $7.50 -4.65; HTFL×15 yday $41.94 → 09:30 $41.50 +6.60; UMAC×19 yday $30.15 → 09:30 $28.59 +29.64; NPWR×335 yday $1.73 → 09:30 $1.70 +10.05; LPTH×43 yday $14.80 → 09:30 $14.01 +33.97; NMAX×58 yday $10.36 → 09:30 $10.31 +2.90; ALOY×43 yday $13.86 → 09:30 $13.19 +28.60; INO×601 yday $1.15 → 09:30 $1.14 +6.01 | — |
| 2026-08-18 09:30 ET | **COVER** | `TNDM` | 214 | $22.16 | $2.76 | $+244.65 | $20,231.88 | ▲ +244.65 after sell → book $10,558.44; vs 09:30 mark -2.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,231.88 | ▼ 09:30 equity $10,346.15 vs yday $10,434.83 (-88.68) | 09:30 open · cash $20,231.88 (unchanged overnight, no fees) · equity $10,346.15 vs prior close $10,434.83 (-88.68) because holdings re-marked: ARX×32 yday $19.56 → 09:30 $19.58 -0.64; OMER×36 yday $17.19 → 09:30 $17.13 +2.16; AIRO×56 yday $8.98 → 09:30 $9.10 -6.72; MXCT×453 yday $1.27 → 09:30 $1.29 -9.06; QMLS×86 yday $6.74 → 09:30 $6.74 +0.00; AVAH×52 yday $12.67 → 09:30 $12.92 -13.00; TBBB×12 yday $48.06 → 09:30 $48.62 -6.72; AMPY×127 yday $4.82 → 09:30 $4.88 -7.62; CAPR×93 yday $7.08 → 09:30 $7.19 -10.23; HTFL×15 yday $45.23 → 09:30 $46.02 -11.85; UMAC×19 yday $30.58 → 09:30 $30.10 +9.12; NPWR×335 yday $1.65 → 09:30 $1.70 -16.75; LPTH×43 yday $14.22 → 09:30 $14.30 -3.44; NMAX×58 yday $11.43 → 09:30 $11.50 -4.06; ALOY×43 yday $13.50 → 09:30 $13.45 +2.15; INO×601 yday $1.20 → 09:30 $1.22 -12.02 | — |
| 2026-08-19 09:30 ET | **COVER** | `ARX` | 32 | $19.58 | $2.09 | $-4.53 | $19,603.23 | ▼ -4.53 after sell → book $10,344.06; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **COVER** | `OMER` | 36 | $17.13 | $2.10 | $+3.69 | $18,984.45 | ▲ +3.69 after sell → book $10,341.96; vs 09:30 mark -2.10 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AIRO` | 56 | $9.10 | $2.16 | $+108.77 | $18,472.69 | ▲ +108.77 after sell → book $10,339.80; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `MXCT` | 453 | $1.29 | $5.84 | $+33.51 | $17,882.48 | ▲ +33.51 after sell → book $10,333.96; vs 09:30 mark -5.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `QMLS` | 86 | $6.74 | $2.25 | $+42.76 | $17,300.59 | ▲ +42.76 after sell → book $10,331.71; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AVAH` | 52 | $12.92 | $2.15 | $-56.85 | $16,626.61 | ▼ -56.85 after sell → book $10,329.57; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `TBBB` | 12 | $48.62 | $2.03 | $-1.69 | $16,041.14 | ▼ -1.69 after sell → book $10,327.54; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **COVER** | `AMPY` | 127 | $4.88 | $2.37 | $+2.83 | $15,419.01 | ▲ +2.83 after sell → book $10,325.17; vs 09:30 mark -2.37 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,419.01 | ▲ 09:30 equity $10,436.28 vs yday $10,375.44 (+60.84) | 09:30 open · cash $15,419.01 (unchanged overnight, no fees) · equity $10,436.28 vs prior close $10,375.44 (+60.84) because holdings re-marked: CAPR×93 yday $7.98 → 09:30 $7.66 +29.76; HTFL×15 yday $45.53 → 09:30 $45.90 -5.55; UMAC×19 yday $28.30 → 09:30 $28.32 -0.38; NPWR×335 yday $1.67 → 09:30 $1.64 +10.05; LPTH×43 yday $13.24 → 09:30 $13.09 +6.45; NMAX×58 yday $10.91 → 09:30 $10.89 +1.16; ALOY×43 yday $12.51 → 09:30 $12.06 +19.35; INO×601 yday $1.30 → 09:30 $1.30 +0.00 | — |
| 2026-08-20 09:30 ET | **COVER** | `CAPR` | 93 | $7.66 | $2.27 | $-78.05 | $14,704.36 | ▼ -78.05 after sell → book $10,434.01; vs 09:30 mark -2.27 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `HTFL` | 15 | $45.90 | $2.04 | $-74.16 | $14,013.83 | ▼ -74.16 after sell → book $10,431.98; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `UMAC` | 19 | $28.32 | $2.05 | $+76.24 | $13,473.70 | ▲ +76.24 after sell → book $10,429.93; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NPWR` | 335 | $1.64 | $4.32 | $+85.07 | $12,919.98 | ▲ +85.07 after sell → book $10,425.61; vs 09:30 mark -4.32 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `LPTH` | 43 | $13.09 | $2.12 | $+75.27 | $12,354.99 | ▲ +75.27 after sell → book $10,423.49; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `NMAX` | 58 | $10.89 | $2.16 | $+0.27 | $11,721.20 | ▲ +0.27 after sell → book $10,421.32; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `ALOY` | 43 | $12.06 | $2.12 | $+107.52 | $11,200.50 | ▲ +107.52 after sell → book $10,419.20; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **COVER** | `INO` | 601 | $1.30 | $7.75 | $-153.86 | $10,411.45 | ▼ -153.86 after sell → book $10,411.45; vs 09:30 mark -7.75 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SHORT** | `MRNA` | 4 | $150.14 | $2.04 | — | $11,009.97 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+173.9; leftover $650.72 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `AZI` | 474 | $1.37 | $6.22 | — | $11,653.13 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.7; leftover $650.72 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `CYPH` | 565 | $1.15 | $7.41 | — | $12,295.47 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+62.6; leftover $650.72 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BNTX` | 5 | $109.06 | $2.04 | — | $12,838.73 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+22.0; leftover $650.72 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `BTGO` | 98 | $6.61 | $2.33 | — | $13,483.69 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+25.3; leftover $650.72 | join🔴 sector🟡 gen🟢 news🟢 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ASST` | 40 | $16.00 | $2.15 | — | $14,121.55 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+18.9; leftover $650.72 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `PPC` | 21 | $30.65 | $2.09 | — | $14,763.11 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,ohlc_hot; 🔵; ⚪; ret5=+16.5; leftover $650.72 | join🟡 sector🟡 gen🟢 news🟡 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **SHORT** | `ABCL` | 55 | $11.81 | $2.19 | — | $15,410.74 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; 🔵; ⚪; ret5=+16.0; leftover $650.72 | join🟢 sector🟡 gen🟢 news🔴 digest🟢 ab🟡 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,410.74 | ▼ 09:30 equity $10,208.28 vs yday $10,383.71 (-175.43) | 09:30 open · cash $15,410.74 (unchanged overnight, no fees) · equity $10,208.28 vs prior close $10,383.71 (-175.43) because holdings re-marked: MRNA×4 yday $133.32 → 09:30 $133.11 +0.84; AZI×474 yday $1.44 → 09:30 $1.46 -9.48; CYPH×565 yday $1.19 → 09:30 $1.32 -73.45; BNTX×5 yday $110.89 → 09:30 $110.92 -0.15; BTGO×98 yday $6.60 → 09:30 $6.95 -34.30; ASST×40 yday $16.13 → 09:30 $17.66 -61.20; PPC×21 yday $31.24 → 09:30 $31.13 +2.31; ABCL×55 yday $11.57 → 09:30 $11.57 +0.00 | — |
| 2026-08-21 09:30 ET | **SHORT** | `AU` | 6 | $119.43 | $2.05 | — | $16,125.27 | — | ret_5>15; gate ret_5_min=15.0; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $729.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `AEM` | 3 | $216.30 | $2.04 | — | $16,772.13 | — | ret_5>15; gate ret_5_min=15.0; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $729.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `ARCT` | 65 | $11.13 | $2.23 | — | $17,493.36 | — | ret_5>15; gate ret_5_min=15.0; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $729.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `INDP` | 524 | $1.39 | $6.88 | — | $18,214.84 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+30.2; leftover $729.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `CAN` | 2480 | $0.29 | $15.17 | — | $18,928.79 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+30.4; leftover $729.16 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `DFDV` | 180 | $4.04 | $2.59 | — | $19,653.40 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+23.6; leftover $729.16 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SHORT** | `TEM` | 11 | $65.60 | $2.06 | — | $20,372.94 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+22.8; leftover $729.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,372.94 | ▼ 09:30 equity $9,450.17 vs yday $9,706.10 (-255.93) | 09:30 open · cash $20,372.94 (unchanged overnight, no fees) · equity $9,450.17 vs prior close $9,706.10 (-255.93) because holdings re-marked: MRNA×4 yday $145.13 → 09:30 $142.70 +9.72; AZI×474 yday $1.45 → 09:30 $1.46 -4.74; CYPH×565 yday $1.42 → 09:30 $1.83 -231.65; BNTX×5 yday $116.57 → 09:30 $114.11 +12.30; BTGO×98 yday $6.84 → 09:30 $6.87 -2.94; ASST×40 yday $18.22 → 09:30 $18.76 -21.60; PPC×21 yday $32.25 → 09:30 $32.50 -5.25; ABCL×55 yday $11.32 → 09:30 $10.97 +19.25; AU×6 yday $121.22 → 09:30 $120.50 +4.32; AEM×3 yday $216.06 → 09:30 $217.03 -2.91; ARCT×65 yday $13.45 → 09:30 $13.26 +12.35; INDP×524 yday $1.29 → 09:30 $1.24 +26.20; CAN×2480 yday $0.35 → 09:30 $0.38 -62.00; DFDV×180 yday $3.94 → 09:30 $4.15 -37.80; TEM×11 yday $72.69 → 09:30 $70.07 +28.82 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $20,372.94 | ▼ 09:30 equity $9,566.91 vs yday $9,671.54 (-104.63) | 09:30 open · cash $20,372.94 (unchanged overnight, no fees) · equity $9,566.91 vs prior close $9,671.54 (-104.63) because holdings re-marked: MRNA×4 yday $139.27 → 09:30 $141.19 -7.68; AZI×474 yday $1.40 → 09:30 $1.33 +33.18; CYPH×565 yday $1.64 → 09:30 $1.70 -33.90; BNTX×5 yday $111.34 → 09:30 $113.13 -8.95; BTGO×98 yday $6.97 → 09:30 $6.89 +7.84; ASST×40 yday $19.82 → 09:30 $20.90 -43.20; PPC×21 yday $32.22 → 09:30 $31.76 +9.66; ABCL×55 yday $10.52 → 09:30 $10.77 -13.75; AU×6 yday $118.66 → 09:30 $119.46 -4.80; AEM×3 yday $214.08 → 09:30 $200.48 +40.80; ARCT×65 yday $13.76 → 09:30 $14.34 -37.70; INDP×524 yday $1.16 → 09:30 $1.18 -10.48; CAN×2480 yday $0.37 → 09:30 $0.38 -24.80; DFDV×180 yday $4.19 → 09:30 $4.29 -18.00; TEM×11 yday $67.10 → 09:30 $66.45 +7.15 | — |
| 2026-08-25 09:30 ET | **COVER** | `MRNA` | 4 | $141.19 | $2.00 | $+31.76 | $19,806.17 | ▲ +31.76 after sell → book $9,564.90; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `AZI` | 474 | $1.33 | $6.11 | $+6.62 | $19,169.64 | ▲ +6.62 after sell → book $9,558.79; vs 09:30 mark -6.11 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `BNTX` | 5 | $113.13 | $2.00 | $-24.39 | $18,601.98 | ▼ -24.39 after sell → book $9,556.78; vs 09:30 mark -2.01 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `BTGO` | 98 | $6.89 | $2.28 | $-32.54 | $17,924.48 | ▼ -32.54 after sell → book $9,554.50; vs 09:30 mark -2.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `PPC` | 21 | $31.76 | $2.05 | $-27.45 | $17,255.47 | ▼ -27.45 after sell → book $9,552.45; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **COVER** | `ABCL` | 55 | $10.77 | $2.15 | $+53.13 | $16,660.96 | ▲ +53.13 after sell → book $9,550.29; vs 09:30 mark -2.16 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SHORT** | `SUJA` | 90 | $8.79 | $2.31 | — | $17,449.76 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $795.86 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `FWDI` | 132 | $5.99 | $2.44 | — | $18,238.00 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+20.7; leftover $795.86 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `DEFT` | 1243 | $0.64 | $11.92 | — | $19,021.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $795.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `GORO` | 225 | $3.53 | $2.97 | — | $19,812.87 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+16.0; leftover $795.86 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `BMNR` | 32 | $24.73 | $2.13 | — | $20,602.11 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; ret5=+26.3; leftover $795.86 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SHORT** | `RUM` | 85 | $9.36 | $2.29 | — | $21,395.41 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+21.3; leftover $795.86 | join🔴 sector🟡 gen🟡 news🟢 digest🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,395.41 | ▲ 09:30 equity $9,663.03 vs yday $9,663.03 (+0.00) | 09:30 open · cash $21,395.41 (unchanged overnight, no fees) · equity $9,663.03 vs prior close $9,663.03 (+0.00) because holdings re-marked: CYPH×565 yday $1.64 → 09:30 $1.64 +0.00; ASST×40 yday $20.20 → 09:30 $20.20 +0.00; AU×6 yday $118.55 → 09:30 $118.55 +0.00; AEM×3 yday $215.40 → 09:30 $215.40 +0.00; ARCT×65 yday $14.21 → 09:30 $14.21 +0.00; INDP×524 yday $1.25 → 09:30 $1.25 +0.00; CAN×2480 yday $0.36 → 09:30 $0.36 +0.00; DFDV×180 yday $4.16 → 09:30 $4.16 +0.00; TEM×11 yday $66.98 → 09:30 $66.98 +0.00; SUJA×90 yday $8.54 → 09:30 $8.54 +0.00; FWDI×132 yday $5.86 → 09:30 $5.86 +0.00; DEFT×1243 yday $0.62 → 09:30 $0.62 +0.00; GORO×225 yday $3.56 → 09:30 $3.56 +0.00; BMNR×32 yday $24.21 → 09:30 $24.21 +0.00; RUM×85 yday $9.35 → 09:30 $9.35 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $21,395.41 | ▼ 09:30 equity $9,340.30 vs yday $9,526.23 (-185.93) | 09:30 open · cash $21,395.41 (unchanged overnight, no fees) · equity $9,340.30 vs prior close $9,526.23 (-185.93) because holdings re-marked: CYPH×565 yday $1.64 → 09:30 $1.60 +22.60; ASST×40 yday $20.20 → 09:30 $20.72 -20.80; AU×6 yday $118.55 → 09:30 $119.80 -7.50; AEM×3 yday $215.40 → 09:30 $219.50 -12.30; ARCT×65 yday $14.21 → 09:30 $15.35 -74.10; INDP×524 yday $1.25 → 09:30 $1.09 +83.84; CAN×2480 yday $0.36 → 09:30 $0.40 -99.20; DFDV×180 yday $4.16 → 09:30 $4.35 -34.20; TEM×11 yday $66.98 → 09:30 $67.48 -5.50; SUJA×90 yday $8.54 → 09:30 $9.39 -76.50; FWDI×132 yday $5.86 → 09:30 $5.97 -14.52; DEFT×1243 yday $0.62 → 09:30 $0.60 +24.86; GORO×225 yday $3.56 → 09:30 $3.77 -47.25; BMNR×32 yday $24.21 → 09:30 $24.24 -0.96; RUM×85 yday $9.35 → 09:30 $10.07 -61.20 | — |
| 2026-08-27 09:30 ET | **COVER** | `CYPH` | 565 | $1.60 | $7.29 | $-268.95 | $20,484.13 | ▼ -268.95 after sell → book $9,333.02; vs 09:30 mark -7.28 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `ASST` | 40 | $20.72 | $2.11 | $-193.06 | $19,653.22 | ▼ -193.06 after sell → book $9,330.91; vs 09:30 mark -2.11 | dropped from list after 5 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `AU` | 6 | $119.80 | $2.01 | $-6.28 | $18,932.41 | ▼ -6.28 after sell → book $9,328.90; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `AEM` | 3 | $219.50 | $2.00 | $-13.64 | $18,271.91 | ▼ -13.64 after sell → book $9,326.90; vs 09:30 mark -2.00 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `ARCT` | 65 | $15.35 | $2.19 | $-278.71 | $17,271.97 | ▼ -278.71 after sell → book $9,324.71; vs 09:30 mark -2.19 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `INDP` | 524 | $1.09 | $6.76 | $+143.56 | $16,694.05 | ▲ +143.56 after sell → book $9,317.95; vs 09:30 mark -6.76 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `CAN` | 2480 | $0.40 | $17.36 | $-295.41 | $15,684.69 | ▼ -295.41 after sell → book $9,300.59; vs 09:30 mark -17.36 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `DFDV` | 180 | $4.35 | $2.53 | $-60.92 | $14,899.16 | ▼ -60.92 after sell → book $9,298.06; vs 09:30 mark -2.53 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **COVER** | `TEM` | 11 | $67.48 | $2.02 | $-24.77 | $14,154.86 | ▼ -24.77 after sell → book $9,296.04; vs 09:30 mark -2.02 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,154.86 | ▼ 09:30 equity $9,273.46 vs yday $9,393.71 (-120.25) | 09:30 open · cash $14,154.86 (unchanged overnight, no fees) · equity $9,273.46 vs prior close $9,393.71 (-120.25) because holdings re-marked: SUJA×90 yday $9.44 → 09:30 $9.41 +2.70; FWDI×132 yday $5.93 → 09:30 $6.39 -60.72; DEFT×1243 yday $0.59 → 09:30 $0.60 -12.43; GORO×225 yday $3.56 → 09:30 $3.59 -6.75; BMNR×32 yday $24.91 → 09:30 $25.91 -32.00; RUM×85 yday $9.38 → 09:30 $9.51 -11.05 | — |
| 2026-08-28 09:30 ET | **COVER** | `SUJA` | 90 | $9.41 | $2.26 | $-60.37 | $13,305.70 | ▼ -60.37 after sell → book $9,271.20; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `FWDI` | 132 | $6.39 | $2.39 | $-57.63 | $12,459.84 | ▼ -57.63 after sell → book $9,268.82; vs 09:30 mark -2.38 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `GORO` | 225 | $3.59 | $2.90 | $-19.37 | $11,649.18 | ▼ -19.37 after sell → book $9,265.91; vs 09:30 mark -2.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `BMNR` | 32 | $25.91 | $2.09 | $-41.97 | $10,817.98 | ▼ -41.97 after sell → book $9,263.83; vs 09:30 mark -2.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **COVER** | `RUM` | 85 | $9.51 | $2.25 | $-17.29 | $10,007.38 | ▼ -17.29 after sell → book $9,261.58; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SHORT** | `FIGR` | 17 | $37.42 | $2.08 | — | $10,641.44 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; ret5=+24.4; leftover $661.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `XHG` | 162 | $4.06 | $2.53 | — | $11,296.63 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $661.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `ERO` | 16 | $39.20 | $2.08 | — | $11,921.76 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.6; leftover $661.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟢 |
| 2026-08-28 09:30 ET | **SHORT** | `TRLV` | 58 | $11.38 | $2.20 | — | $12,579.59 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+15.0; leftover $661.54 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `FUTU` | 5 | $128.00 | $2.04 | — | $13,217.55 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.5; leftover $661.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `TXG` | 10 | $64.10 | $2.06 | — | $13,856.49 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $661.54 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SHORT** | `WPM` | 4 | $155.89 | $2.04 | — | $14,478.02 | — | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+17.6; leftover $661.54 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14,478.02 | ▲ 09:30 equity $9,376.14 vs yday $9,227.96 (+148.18) | 09:30 open · cash $14,478.02 (unchanged overnight, no fees) · equity $9,376.14 vs prior close $9,227.96 (+148.18) because holdings re-marked: DEFT×1243 yday $0.65 → 09:30 $0.62 +37.29; FIGR×17 yday $38.02 → 09:30 $35.50 +42.84; XHG×162 yday $3.80 → 09:30 $3.44 +58.32; ERO×16 yday $39.82 → 09:30 $38.60 +19.52; TRLV×58 yday $11.03 → 09:30 $12.41 -80.04; FUTU×5 yday $124.57 → 09:30 $122.82 +8.75; TXG×10 yday $64.85 → 09:30 $60.90 +39.50; WPM×4 yday $157.99 → 09:30 $152.49 +22.00 | — |
| 2026-08-31 09:30 ET | **COVER** | `DEFT` | 1243 | $0.62 | $11.44 | $+1.50 | $13,695.92 | ▲ +1.50 after sell → book $9,364.70; vs 09:30 mark -11.44 | dropped from list after 4 sess (min 3) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 heat🔴 vol🟡 buy🟡 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,695.92 | ▲ 09:30 equity $9,369.54 vs yday $9,340.85 (+28.69) | 09:30 open · cash $13,695.92 (unchanged overnight, no fees) · equity $9,369.54 vs prior close $9,340.85 (+28.69) because holdings re-marked: FIGR×17 yday $36.41 → 09:30 $36.80 -6.63; XHG×162 yday $3.44 → 09:30 $3.52 -12.96; ERO×16 yday $38.49 → 09:30 $37.30 +19.04; TRLV×58 yday $12.41 → 09:30 $11.89 +30.16; FUTU×5 yday $124.04 → 09:30 $122.22 +9.10; TXG×10 yday $61.40 → 09:30 $62.99 -15.90; WPM×4 yday $152.25 → 09:30 $150.78 +5.88 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $13,695.92 | ▲ 09:30 equity $9,483.82 vs yday $9,448.14 (+35.68) | 09:30 open · cash $13,695.92 (unchanged overnight, no fees) · equity $9,483.82 vs prior close $9,448.14 (+35.68) because holdings re-marked: FIGR×17 yday $35.70 → 09:30 $35.46 +4.08; XHG×162 yday $3.43 → 09:30 $3.48 -8.10; ERO×16 yday $36.01 → 09:30 $35.95 +0.96; TRLV×58 yday $11.89 → 09:30 $11.54 +20.30; FUTU×5 yday $120.88 → 09:30 $119.82 +5.30; TXG×10 yday $62.92 → 09:30 $61.79 +11.30; WPM×4 yday $146.46 → 09:30 $146.00 +1.84 | — |
| 2026-09-02 09:30 ET | **COVER** | `FIGR` | 17 | $35.46 | $2.04 | $+29.20 | $13,091.06 | ▲ +29.20 after sell → book $9,481.78; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **COVER** | `TXG` | 10 | $61.79 | $2.02 | $+19.02 | $12,471.14 | ▲ +19.02 after sell → book $9,479.76; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **COVER** | `WPM` | 4 | $146.00 | $2.00 | $+35.52 | $11,885.14 | ▲ +35.52 after sell → book $9,477.76; vs 09:30 mark -2.00 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $11,885.14 | ▼ 09:30 equity $9,456.34 vs yday $9,482.08 (-25.74) | 09:30 open · cash $11,885.14 (unchanged overnight, no fees) · equity $9,456.34 vs prior close $9,482.08 (-25.74) because holdings re-marked: XHG×162 yday $3.51 → 09:30 $3.57 -9.72; ERO×16 yday $34.82 → 09:30 $35.62 -12.80; TRLV×58 yday $11.74 → 09:30 $11.78 -2.32; FUTU×5 yday $119.28 → 09:30 $119.46 -0.90 | — |
| 2026-09-03 09:30 ET | **COVER** | `FUTU` | 5 | $119.46 | $2.00 | $+38.65 | $11,285.83 | ▲ +38.65 after sell → book $9,454.33; vs 09:30 mark -2.01 | dropped from list after 4 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `DEFT` | 1411 | $0.67 | $13.96 | — | $12,217.25 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $945.43 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `MRNA` | 6 | $151.40 | $2.05 | — | $13,123.59 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $945.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ARCT` | 57 | $16.46 | $2.21 | — | $14,059.61 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $945.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `ALEC` | 393 | $2.40 | $5.17 | — | $14,997.64 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.4; leftover $945.43 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **SHORT** | `CAN` | 3151 | $0.30 | $19.46 | — | $15,923.47 | — | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ret5=+54.3; leftover $945.43 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🔴 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15,923.47 | ▼ 09:30 equity $9,231.99 vs yday $9,329.49 (-97.50) | 09:30 open · cash $15,923.47 (unchanged overnight, no fees) · equity $9,231.99 vs prior close $9,329.49 (-97.50) because holdings re-marked: XHG×162 yday $3.32 → 09:30 $3.38 -9.72; ERO×16 yday $34.76 → 09:30 $35.82 -16.96; TRLV×58 yday $11.69 → 09:30 $11.89 -11.60; DEFT×1411 yday $0.65 → 09:30 $0.65 +0.00; MRNA×6 yday $150.81 → 09:30 $145.95 +29.16; ARCT×57 yday $16.74 → 09:30 $16.77 -1.71; ALEC×393 yday $2.72 → 09:30 $2.70 +7.86; CAN×3151 yday $0.31 → 09:30 $0.34 -94.53 | — |
| 2026-09-04 09:30 ET | **SHORT** | `HQ` | 90 | $17.06 | $2.33 | — | $17,456.54 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.3; leftover $1538.67 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `OABI` | 302 | $5.08 | $4.00 | — | $18,986.70 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1538.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **SHORT** | `BRR` | 651 | $2.36 | $8.56 | — | $20,514.50 | — | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1538.67 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-14 | `TNDM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TNDM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AVAH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TBBB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AMPY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AVAH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `TBBB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AMPY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `HTFL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `UMAC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `LPTH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `NMAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALOY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `INO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SNDK` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `SMTC` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `QTRX` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ALM` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `HTFL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `UMAC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `LPTH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALOY` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `INO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `KURA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AZI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BNTX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BTGO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `PPC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABCL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `MRNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AZI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BNTX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTGO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `PPC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABCL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `INDP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `DFDV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `TEM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `USDE` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DEFT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SLS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `COIN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `INDP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CAN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `DFDV` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `TEM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AU` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AEM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CAN` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DFDV` | no_price | no 09:30 open — carry |
| 2026-08-26 | `TEM` | no_price | no 09:30 open — carry |
| 2026-08-26 | `FWDI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RUM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SENS` | no_price | no 09:30 open |
| 2026-08-26 | `BRR` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `XHG` | no_price | no 09:30 open |
| 2026-08-26 | `ERO` | no_price | no 09:30 open |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `FWDI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `RUM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `FIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TRLV` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `FUTU` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TXG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `FWDI` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `MSTR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `FIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `FUTU` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TXG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `KOS` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SUJA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CYPH` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `PURR` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `ALEC` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `DEFT` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `PRQR` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `USDE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `SUJA` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `DEFT` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CAN` | min_hold | dropped but min-hold 1/3 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `XHG` | 162 | 2026-08-28 @ $4.06 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.1; leftover $661.54 |
| `ERO` | 16 | 2026-08-28 @ $39.20 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+16.6; leftover $661.54 |
| `TRLV` | 58 | 2026-08-28 @ $11.38 | ret_5>15; gate ret_5_min=15.0; list ohlc_hot; ret5=+15.0; leftover $661.54 |
| `DEFT` | 1411 | 2026-09-03 @ $0.67 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $945.43 |
| `MRNA` | 6 | 2026-09-03 @ $151.40 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $945.43 |
| `ARCT` | 57 | 2026-09-03 @ $16.46 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $945.43 |
| `ALEC` | 393 | 2026-09-03 @ $2.40 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ret5=+20.4; leftover $945.43 |
| `CAN` | 3151 | 2026-09-03 @ $0.30 | ret_5>15; gate ret_5_min=15.0; list yday_mover; 🔵; ret5=+54.3; leftover $945.43 |
| `HQ` | 90 | 2026-09-04 @ $17.06 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; ret5=+17.3; leftover $1538.67 |
| `OABI` | 302 | 2026-09-04 @ $5.08 | ret_5>15; gate ret_5_min=15.0; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1538.67 |
| `BRR` | 651 | 2026-09-04 @ $2.36 | ret_5>15; gate ret_5_min=15.0; list yday_gainer; 🔵; ⚪; ret5=+28.0; leftover $1538.67 |
