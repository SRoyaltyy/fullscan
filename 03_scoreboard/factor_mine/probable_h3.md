# Factor mine action — `probable_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-6.50%** ($9,350) · signal-only (no cash/fees) was +0.05%. Starts YES **4/17**. Fills 92 · skips 160 · realized $-683.25.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $10.49.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | — | $269.08 | $9,985.46 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $269.08 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | $10,059.20 | +73.74 | ABX, FCEL, VERA, BW, OCC, ALM | — | $104.70 | $9,954.02 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | 09:30 open · cash $269.08 (unchanged overnight, no fees) · equity $10,059.20 vs prior close $9,985.46 (+73.74) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; FOSL×221 yday $5.57 → 09:30 $5.50 -15.47; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRS×370 yday $3.43 → 09:30 $3.40 -12.95; ALGM×28 yday $44.39 → 09:30 $45.32 +26.04 |
| 2026-08-18 | -6.20 | $104.70 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | $9,758.08 | -195.94 | — | — | $104.70 | $9,500.71 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | 09:30 open · cash $104.70 (unchanged overnight, no fees) · equity $9,758.08 vs prior close $9,954.02 (-195.94) because holdings re-marked: ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; WWW×60 yday $19.83 → 09:30 $19.95 +7.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; WDC×2 yday $536.01 → 09:30 $496.07 -79.88; FOSL×221 yday $5.74 → 09:30 $5.78 +8.84; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; AIRS×370 yday $3.08 → 09:30 $3.01 -27.75; ALGM×28 yday $44.25 → 09:30 $42.54 -47.88; ABX×3 yday $9.12 → 09:30 $9.03 -0.27; FCEL×1 yday $22.36 → 09:30 $21.18 -1.18; VERA×1 yday $31.63 → 09:30 $31.31 -0.32; BW×3 yday $9.92 → 09:30 $9.60 -0.96; OCC×1 yday $17.12 → 09:30 $16.20 -0.92; ALM×2 yday $16.36 → 09:30 $15.78 -1.16 |
| 2026-08-19 | -7.20 | $104.70 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | $9,522.41 | +21.70 | — | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | $9,341.09 | $9,495.16 | ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | 09:30 open · cash $104.70 (unchanged overnight, no fees) · equity $9,522.41 vs prior close $9,500.71 (+21.70) because holdings re-marked: ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; WWW×60 yday $19.99 → 09:30 $20.08 +5.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; WDC×2 yday $496.16 → 09:30 $494.28 -3.76; FOSL×221 yday $5.50 → 09:30 $5.54 +8.84; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; AIRS×370 yday $2.69 → 09:30 $2.71 +5.55; ALGM×28 yday $39.39 → 09:30 $40.00 +17.08; ABX×3 yday $9.01 → 09:30 $9.08 +0.21; FCEL×1 yday $21.70 → 09:30 $21.48 -0.22; VERA×1 yday $32.28 → 09:30 $32.88 +0.60; BW×3 yday $9.14 → 09:30 $9.14 +0.00; OCC×1 yday $16.20 → 09:30 $16.21 +0.01; ALM×2 yday $15.60 → 09:30 $16.05 +0.90 |
| 2026-08-20 | +1.12 | $9,341.09 | ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | $9,493.85 | -1.31 | MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | ABX, FCEL, VERA, BW, OCC, ALM | $87.71 | $9,549.23 | MRVI×160, DNA×159, MSTR×10, EXK×110, SCZM×125, NG×141, BLSH×40, HYMC×43 | 09:30 open · cash $9,341.09 (unchanged overnight, no fees) · equity $9,493.85 vs prior close $9,495.16 (-1.31) because holdings re-marked: ABX×3 yday $9.15 → 09:30 $9.13 -0.06; FCEL×1 yday $20.30 → 09:30 $20.21 -0.09; VERA×1 yday $32.27 → 09:30 $32.30 +0.02; BW×3 yday $9.11 → 09:30 $9.05 -0.18; OCC×1 yday $14.36 → 09:30 $14.10 -0.26; ALM×2 yday $16.18 → 09:30 $15.81 -0.74 |
| 2026-08-21 | +3.25 | $87.71 | MRVI×160, DNA×159, MSTR×10, EXK×110, SCZM×125, NG×141, BLSH×40, HYMC×43 | $9,893.84 | +344.61 | BTBT, ENHA, ORBS, GORO, QTRX | — | $26.43 | $9,834.82 | MRVI×160, DNA×159, MSTR×10, EXK×110, SCZM×125, NG×141, BLSH×40, HYMC×43, BTBT×7, ENHA×7, ORBS×14, GORO×4, QTRX×4 | 09:30 open · cash $87.71 (unchanged overnight, no fees) · equity $9,893.84 vs prior close $9,549.23 (+344.61) because holdings re-marked: MRVI×160 yday $8.26 → 09:30 $8.20 -9.60; DNA×159 yday $6.96 → 09:30 $7.09 +20.67; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×110 yday $10.97 → 09:30 $11.34 +40.70; SCZM×125 yday $9.76 → 09:30 $10.26 +62.50; NG×141 yday $8.66 → 09:30 $9.02 +50.76; BLSH×40 yday $28.44 → 09:30 $29.75 +52.40; HYMC×43 yday $26.14 → 09:30 $27.40 +54.18 |
| 2026-08-24 | -5.17 | $26.43 | MRVI×160, DNA×159, MSTR×10, EXK×110, SCZM×125, NG×141, BLSH×40, HYMC×43, BTBT×7, ENHA×7, ORBS×14, GORO×4, QTRX×4 | $9,903.59 | +68.77 | — | — | $26.43 | $9,785.53 | MRVI×160, DNA×159, MSTR×10, EXK×110, SCZM×125, NG×141, BLSH×40, HYMC×43, BTBT×7, ENHA×7, ORBS×14, GORO×4, QTRX×4 | 09:30 open · cash $26.43 (unchanged overnight, no fees) · equity $9,903.59 vs prior close $9,834.82 (+68.77) because holdings re-marked: MRVI×160 yday $8.70 → 09:30 $8.59 -17.60; DNA×159 yday $7.40 → 09:30 $7.26 -22.26; MSTR×10 yday $119.25 → 09:30 $121.76 +25.10; EXK×110 yday $10.62 → 09:30 $11.01 +42.90; SCZM×125 yday $9.68 → 09:30 $9.82 +18.12; NG×141 yday $8.72 → 09:30 $8.89 +23.97; BLSH×40 yday $30.41 → 09:30 $30.18 -9.20; HYMC×43 yday $27.07 → 09:30 $27.24 +7.31; BTBT×7 yday $1.53 → 09:30 $1.55 +0.14; ENHA×7 yday $1.72 → 09:30 $1.74 +0.14; ORBS×14 yday $0.88 → 09:30 $0.89 +0.14; GORO×4 yday $3.19 → 09:30 $3.20 +0.04; QTRX×4 yday $2.99 → 09:30 $2.98 -0.04 |
| 2026-08-25 | +1.80 | $26.43 | MRVI×160, DNA×159, MSTR×10, EXK×110, SCZM×125, NG×141, BLSH×40, HYMC×43, BTBT×7, ENHA×7, ORBS×14, GORO×4, QTRX×4 | $9,794.41 | +8.88 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH, HYMC | $0.80 | $9,897.61 | BTBT×7, ENHA×7, ORBS×14, GORO×4, QTRX×4, BMEA×749, NPWR×607, PUSA×328, ALVO×232, CAPR×178, ALIT×81, ZURA×190, SAFX×3204 | 09:30 open · cash $26.43 (unchanged overnight, no fees) · equity $9,794.41 vs prior close $9,785.53 (+8.88) because holdings re-marked: MRVI×160 yday $8.26 → 09:30 $8.31 +8.00; DNA×159 yday $6.98 → 09:30 $6.82 -25.44; MSTR×10 yday $124.59 → 09:30 $125.56 +9.70; EXK×110 yday $10.74 → 09:30 $10.72 -2.20; SCZM×125 yday $9.53 → 09:30 $9.57 +5.00; NG×141 yday $9.24 → 09:30 $9.34 +14.10; BLSH×40 yday $30.88 → 09:30 $31.00 +4.80; HYMC×43 yday $25.84 → 09:30 $25.73 -4.73; BTBT×7 yday $1.56 → 09:30 $1.55 -0.07; ENHA×7 yday $1.69 → 09:30 $1.65 -0.28; ORBS×14 yday $0.85 → 09:30 $0.85 +0.00; GORO×4 yday $3.57 → 09:30 $3.53 -0.16; QTRX×4 yday $2.76 → 09:30 $2.80 +0.16 |
| 2026-08-26 | +2.02 | $0.80 | BTBT×7, ENHA×7, ORBS×14, GORO×4, QTRX×4, BMEA×749, NPWR×607, PUSA×328, ALVO×232, CAPR×178, ALIT×81, ZURA×190, SAFX×3204 | $9,897.61 | -0.00 | — | — | $0.80 | $9,722.40 | BTBT×7, ENHA×7, ORBS×14, GORO×4, QTRX×4, BMEA×749, NPWR×607, PUSA×328, ALVO×232, CAPR×178, ALIT×81, ZURA×190, SAFX×3204 | 09:30 open · cash $0.80 (unchanged overnight, no fees) · equity $9,897.61 vs prior close $9,897.61 (-0.00) because holdings re-marked: BTBT×7 yday $1.53 → 09:30 $1.53 +0.00; ENHA×7 yday $1.66 → 09:30 $1.66 +0.00; ORBS×14 yday $0.84 → 09:30 $0.84 +0.00; GORO×4 yday $3.56 → 09:30 $3.56 +0.00; QTRX×4 yday $2.80 → 09:30 $2.80 +0.00; BMEA×749 yday $1.61 → 09:30 $1.61 +0.00; NPWR×607 yday $2.02 → 09:30 $2.02 +0.00; PUSA×328 yday $3.91 → 09:30 $3.91 +0.00; ALVO×232 yday $5.25 → 09:30 $5.25 +0.00; CAPR×178 yday $7.19 → 09:30 $7.19 +0.00; ALIT×81 yday $14.87 → 09:30 $14.87 +0.00; ZURA×190 yday $6.50 → 09:30 $6.50 +0.00; SAFX×3204 yday $0.37 → 09:30 $0.37 +0.00 |
| 2026-08-27 | — | $0.80 | BTBT×7, ENHA×7, ORBS×14, GORO×4, QTRX×4, BMEA×749, NPWR×607, PUSA×328, ALVO×232, CAPR×178, ALIT×81, ZURA×190, SAFX×3204 | $9,922.23 | +199.83 | — | BTBT, ENHA, ORBS, GORO, QTRX | $59.71 | $10,055.56 | BMEA×749, NPWR×607, PUSA×328, ALVO×232, CAPR×178, ALIT×81, ZURA×190, SAFX×3204 | 09:30 open · cash $0.80 (unchanged overnight, no fees) · equity $9,922.23 vs prior close $9,722.40 (+199.83) because holdings re-marked: BTBT×7 yday $1.53 → 09:30 $1.53 +0.00; ENHA×7 yday $1.66 → 09:30 $1.63 -0.21; ORBS×14 yday $0.84 → 09:30 $0.80 -0.56; GORO×4 yday $3.56 → 09:30 $3.77 +0.84; QTRX×4 yday $2.80 → 09:30 $2.83 +0.12; BMEA×749 yday $1.61 → 09:30 $1.75 +104.86; NPWR×607 yday $2.02 → 09:30 $1.93 -54.63; PUSA×328 yday $3.91 → 09:30 $3.84 -22.96; ALVO×232 yday $5.25 → 09:30 $4.98 -62.64; CAPR×178 yday $7.19 → 09:30 $8.29 +195.80; ALIT×81 yday $14.87 → 09:30 $14.85 -1.62; ZURA×190 yday $6.50 → 09:30 $6.13 -70.30; SAFX×3204 yday $0.37 → 09:30 $0.35 -64.08 |
| 2026-08-28 | +0.75 | $59.71 | BMEA×749, NPWR×607, PUSA×328, ALVO×232, CAPR×178, ALIT×81, ZURA×190, SAFX×3204 | $10,078.94 | +23.38 | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | BMEA, NPWR, PUSA, ALVO, ALIT, ZURA, SAFX | $67.68 | $9,972.03 | CAPR×178, ANF×8, BHVN×70, BZ×64, LVWR×868, SEDG×35, SMTC×8, GRRR×75 | 09:30 open · cash $59.71 (unchanged overnight, no fees) · equity $10,078.94 vs prior close $10,055.56 (+23.38) because holdings re-marked: BMEA×749 yday $1.71 → 09:30 $1.74 +22.47; NPWR×607 yday $1.81 → 09:30 $1.83 +12.14; PUSA×328 yday $3.85 → 09:30 $3.86 +3.28; ALVO×232 yday $4.91 → 09:30 $4.88 -6.96; CAPR×178 yday $9.36 → 09:30 $9.19 -30.26; ALIT×81 yday $14.33 → 09:30 $14.54 +17.01; ZURA×190 yday $5.99 → 09:30 $6.02 +5.70; SAFX×3204 yday $0.39 → 09:30 $0.39 +0.00 |
| 2026-08-31 | -5.85 | $67.68 | CAPR×178, ANF×8, BHVN×70, BZ×64, LVWR×868, SEDG×35, SMTC×8, GRRR×75 | $9,593.10 | -378.93 | — | CAPR | $1,745.43 | $9,546.16 | ANF×8, BHVN×70, BZ×64, LVWR×868, SEDG×35, SMTC×8, GRRR×75 | 09:30 open · cash $67.68 (unchanged overnight, no fees) · equity $9,593.10 vs prior close $9,972.03 (-378.93) because holdings re-marked: CAPR×178 yday $10.06 → 09:30 $9.44 -110.36; ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×70 yday $16.12 → 09:30 $15.44 -47.60; BZ×64 yday $18.00 → 09:30 $17.89 -7.04; LVWR×868 yday $1.36 → 09:30 $1.37 +8.68; SEDG×35 yday $33.51 → 09:30 $31.50 -70.35; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×75 yday $15.66 → 09:30 $14.32 -100.50 |
| 2026-09-01 | -6.30 | $1,745.43 | ANF×8, BHVN×70, BZ×64, LVWR×868, SEDG×35, SMTC×8, GRRR×75 | $9,446.98 | -99.18 | — | — | $1,745.43 | $9,353.05 | ANF×8, BHVN×70, BZ×64, LVWR×868, SEDG×35, SMTC×8, GRRR×75 | 09:30 open · cash $1,745.43 (unchanged overnight, no fees) · equity $9,446.98 vs prior close $9,546.16 (-99.18) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BHVN×70 yday $15.40 → 09:30 $15.45 +3.50; BZ×64 yday $17.90 → 09:30 $17.37 -33.92; LVWR×868 yday $1.34 → 09:30 $1.22 -104.16; SEDG×35 yday $31.27 → 09:30 $32.22 +33.25; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×75 yday $14.20 → 09:30 $15.05 +63.75 |
| 2026-09-02 | -3.83 | $1,745.43 | ANF×8, BHVN×70, BZ×64, LVWR×868, SEDG×35, SMTC×8, GRRR×75 | $9,340.95 | -12.10 | — | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | $9,316.76 | $9,316.76 | — | 09:30 open · cash $1,745.43 (unchanged overnight, no fees) · equity $9,340.95 vs prior close $9,353.05 (-12.10) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BHVN×70 yday $15.45 → 09:30 $15.39 -4.20; BZ×64 yday $17.17 → 09:30 $17.29 +7.68; LVWR×868 yday $1.18 → 09:30 $1.19 +8.68; SEDG×35 yday $31.80 → 09:30 $31.87 +2.45; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×75 yday $14.80 → 09:30 $14.75 -3.75 |
| 2026-09-03 | -0.90 | $9,316.76 | — | $9,316.76 | -0.00 | GPRO, FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | — | $10.49 | $9,723.41 | GPRO×954, FRVO×63, CRK×74, MMED×51, CTMX×313, SLN×79, EIX×20, CRDL×539 | 09:30 open · cash $9,316.76 · no holdings · equity $9,316.76 vs prior close $9,316.76 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $10.49 | GPRO×954, FRVO×63, CRK×74, MMED×51, CTMX×313, SLN×79, EIX×20, CRDL×539 | $9,844.86 | +121.45 | — | — | $10.49 | $9,350.26 | GPRO×954, FRVO×63, CRK×74, MMED×51, CTMX×313, SLN×79, EIX×20, CRDL×539 | 09:30 open · cash $10.49 (unchanged overnight, no fees) · equity $9,844.86 vs prior close $9,723.41 (+121.45) because holdings re-marked: GPRO×954 yday $1.69 → 09:30 $1.78 +85.86; FRVO×63 yday $17.98 → 09:30 $18.27 +18.27; CRK×74 yday $15.54 → 09:30 $15.45 -6.66; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; CTMX×313 yday $3.72 → 09:30 $3.73 +3.13; SLN×79 yday $14.79 → 09:30 $14.85 +4.74; EIX×20 yday $55.19 → 09:30 $55.42 +4.60; CRDL×539 yday $2.17 → 09:30 $2.18 +5.39 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WDC` | 2 | $503.50 | $2.00 | — | $5,245.52 | — | baseline list, no extra gate; list probable; 🔵; ⚪; ret5=+7.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `FOSL` | 221 | $5.64 | $2.85 | — | $3,996.23 | — | baseline list, no extra gate; list probable; 🔵; ret5=-4.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $2,756.51 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRS` | 370 | $3.37 | $4.77 | — | $1,504.84 | — | baseline list, no extra gate; list probable; ret5=-29.1; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ALGM` | 28 | $44.06 | $2.07 | — | $269.08 | — | baseline list, no extra gate; list probable; 🔵; ret5=+3.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🔴 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $269.08 | ▲ 09:30 equity $10,059.20 vs yday $9,985.46 (+73.74) | 09:30 open · cash $269.08 (unchanged overnight, no fees) · equity $10,059.20 vs prior close $9,985.46 (+73.74) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; FOSL×221 yday $5.57 → 09:30 $5.50 -15.47; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRS×370 yday $3.43 → 09:30 $3.40 -12.95; ALGM×28 yday $44.39 → 09:30 $45.32 +26.04 | — |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 3 | $9.12 | $0.28 | — | $241.44 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $33.64 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 1 | $22.37 | $0.23 | — | $218.84 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $33.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 1 | $31.30 | $0.32 | — | $187.23 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $33.64 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `BW` | 3 | $10.35 | $0.32 | — | $155.86 | — | baseline list, no extra gate; list probable; ⚪; ret5=+9.8; leftover $33.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `OCC` | 1 | $18.24 | $0.19 | — | $137.43 | — | baseline list, no extra gate; list probable,ohlc_hot; ⚪; ret5=+9.5; leftover $33.64 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ALM` | 2 | $16.20 | $0.33 | — | $104.70 | — | baseline list, no extra gate; list probable,ohlc_hot; 🔵; ⚪; ret5=+6.4; leftover $33.64 | join🟢 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.70 | ▼ 09:30 equity $9,758.08 vs yday $9,954.02 (-195.94) | 09:30 open · cash $104.70 (unchanged overnight, no fees) · equity $9,758.08 vs prior close $9,954.02 (-195.94) because holdings re-marked: ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; WWW×60 yday $19.83 → 09:30 $19.95 +7.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; WDC×2 yday $536.01 → 09:30 $496.07 -79.88; FOSL×221 yday $5.74 → 09:30 $5.78 +8.84; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; AIRS×370 yday $3.08 → 09:30 $3.01 -27.75; ALGM×28 yday $44.25 → 09:30 $42.54 -47.88; ABX×3 yday $9.12 → 09:30 $9.03 -0.27; FCEL×1 yday $22.36 → 09:30 $21.18 -1.18; VERA×1 yday $31.63 → 09:30 $31.31 -0.32; BW×3 yday $9.92 → 09:30 $9.60 -0.96; OCC×1 yday $17.12 → 09:30 $16.20 -0.92; ALM×2 yday $16.36 → 09:30 $15.78 -1.16 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.70 | ▲ 09:30 equity $9,522.41 vs yday $9,500.71 (+21.70) | 09:30 open · cash $104.70 (unchanged overnight, no fees) · equity $9,522.41 vs prior close $9,500.71 (+21.70) because holdings re-marked: ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; WWW×60 yday $19.99 → 09:30 $20.08 +5.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; WDC×2 yday $496.16 → 09:30 $494.28 -3.76; FOSL×221 yday $5.50 → 09:30 $5.54 +8.84; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; AIRS×370 yday $2.69 → 09:30 $2.71 +5.55; ALGM×28 yday $39.39 → 09:30 $40.00 +17.08; ABX×3 yday $9.01 → 09:30 $9.08 +0.21; FCEL×1 yday $21.70 → 09:30 $21.48 -0.22; VERA×1 yday $32.28 → 09:30 $32.88 +0.60; BW×3 yday $9.14 → 09:30 $9.14 +0.00; OCC×1 yday $16.20 → 09:30 $16.21 +0.01; ALM×2 yday $15.60 → 09:30 $16.05 +0.90 | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $1,490.00 | ▲ +131.66 after sell → book $9,518.61; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `WWW` | 60 | $20.08 | $2.19 | $-35.56 | $2,692.61 | ▼ -35.56 after sell → book $9,516.42; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $3,845.83 | ▼ -100.46 after sell → book $9,512.51; vs 09:30 mark -3.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `WDC` | 2 | $494.28 | $2.02 | $-22.45 | $4,832.37 | ▼ -22.45 after sell → book $9,510.49; vs 09:30 mark -2.02 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `FOSL` | 221 | $5.54 | $2.90 | $-27.85 | $6,053.81 | ▼ -27.85 after sell → book $9,507.59; vs 09:30 mark -2.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $7,225.32 | ▼ -68.20 after sell → book $9,505.35; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRS` | 370 | $2.71 | $4.84 | $-253.82 | $8,223.18 | ▼ -253.82 after sell → book $9,500.51; vs 09:30 mark -4.84 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ALGM` | 28 | $40.00 | $2.09 | $-117.85 | $9,341.09 | ▼ -117.85 after sell → book $9,498.42; vs 09:30 mark -2.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,341.09 | ▼ 09:30 equity $9,493.85 vs yday $9,495.16 (-1.31) | 09:30 open · cash $9,341.09 (unchanged overnight, no fees) · equity $9,493.85 vs prior close $9,495.16 (-1.31) because holdings re-marked: ABX×3 yday $9.15 → 09:30 $9.13 -0.06; FCEL×1 yday $20.30 → 09:30 $20.21 -0.09; VERA×1 yday $32.27 → 09:30 $32.30 +0.02; BW×3 yday $9.11 → 09:30 $9.05 -0.18; OCC×1 yday $14.36 → 09:30 $14.10 -0.26; ALM×2 yday $16.18 → 09:30 $15.81 -0.74 | — |
| 2026-08-20 09:30 ET | **SELL** | `ABX` | 3 | $9.13 | $0.30 | $-0.56 | $9,368.17 | ▼ -0.56 after sell → book $9,493.55; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `FCEL` | 1 | $20.21 | $0.23 | $-2.61 | $9,388.16 | ▼ -2.61 after sell → book $9,493.32; vs 09:30 mark -0.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `VERA` | 1 | $32.30 | $0.35 | $+0.33 | $9,420.11 | ▲ +0.33 after sell → book $9,492.98; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `BW` | 3 | $9.05 | $0.30 | $-4.52 | $9,446.96 | ▼ -4.52 after sell → book $9,492.68; vs 09:30 mark -0.30 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `OCC` | 1 | $14.10 | $0.16 | $-4.49 | $9,460.89 | ▼ -4.49 after sell → book $9,492.51; vs 09:30 mark -0.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **SELL** | `ALM` | 2 | $15.81 | $0.34 | $-1.45 | $9,492.17 | ▼ -1.45 after sell → book $9,492.17; vs 09:30 mark -0.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 160 | $7.38 | $2.47 | — | $8,308.90 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1186.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 159 | $7.45 | $2.47 | — | $7,121.88 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1186.52 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $5,987.56 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1186.52 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 110 | $10.77 | $2.32 | — | $4,800.54 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1186.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 125 | $9.46 | $2.37 | — | $3,615.68 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1186.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 141 | $8.38 | $2.41 | — | $2,431.69 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1186.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 40 | $29.20 | $2.11 | — | $1,261.58 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1186.52 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HYMC` | 43 | $27.25 | $2.12 | — | $87.71 | — | baseline list, no extra gate; list probable; 🔵; ret5=+1.6; leftover $1186.52 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $87.71 | ▲ 09:30 equity $9,893.84 vs yday $9,549.23 (+344.61) | 09:30 open · cash $87.71 (unchanged overnight, no fees) · equity $9,893.84 vs prior close $9,549.23 (+344.61) because holdings re-marked: MRVI×160 yday $8.26 → 09:30 $8.20 -9.60; DNA×159 yday $6.96 → 09:30 $7.09 +20.67; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×110 yday $10.97 → 09:30 $11.34 +40.70; SCZM×125 yday $9.76 → 09:30 $10.26 +62.50; NG×141 yday $8.66 → 09:30 $9.02 +50.76; BLSH×40 yday $28.44 → 09:30 $29.75 +52.40; HYMC×43 yday $26.14 → 09:30 $27.40 +54.18 | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 7 | $1.66 | $0.14 | — | $75.95 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $12.53 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 7 | $1.71 | $0.14 | — | $63.84 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $12.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 14 | $0.86 | $0.16 | — | $51.58 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $12.53 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 4 | $3.11 | $0.14 | — | $39.00 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $12.53 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 4 | $3.11 | $0.14 | — | $26.43 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $12.53 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.43 | ▲ 09:30 equity $9,903.59 vs yday $9,834.82 (+68.77) | 09:30 open · cash $26.43 (unchanged overnight, no fees) · equity $9,903.59 vs prior close $9,834.82 (+68.77) because holdings re-marked: MRVI×160 yday $8.70 → 09:30 $8.59 -17.60; DNA×159 yday $7.40 → 09:30 $7.26 -22.26; MSTR×10 yday $119.25 → 09:30 $121.76 +25.10; EXK×110 yday $10.62 → 09:30 $11.01 +42.90; SCZM×125 yday $9.68 → 09:30 $9.82 +18.12; NG×141 yday $8.72 → 09:30 $8.89 +23.97; BLSH×40 yday $30.41 → 09:30 $30.18 -9.20; HYMC×43 yday $27.07 → 09:30 $27.24 +7.31; BTBT×7 yday $1.53 → 09:30 $1.55 +0.14; ENHA×7 yday $1.72 → 09:30 $1.74 +0.14; ORBS×14 yday $0.88 → 09:30 $0.89 +0.14; GORO×4 yday $3.19 → 09:30 $3.20 +0.04; QTRX×4 yday $2.99 → 09:30 $2.98 -0.04 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $26.43 | ▲ 09:30 equity $9,794.41 vs yday $9,785.53 (+8.88) | 09:30 open · cash $26.43 (unchanged overnight, no fees) · equity $9,794.41 vs prior close $9,785.53 (+8.88) because holdings re-marked: MRVI×160 yday $8.26 → 09:30 $8.31 +8.00; DNA×159 yday $6.98 → 09:30 $6.82 -25.44; MSTR×10 yday $124.59 → 09:30 $125.56 +9.70; EXK×110 yday $10.74 → 09:30 $10.72 -2.20; SCZM×125 yday $9.53 → 09:30 $9.57 +5.00; NG×141 yday $9.24 → 09:30 $9.34 +14.10; BLSH×40 yday $30.88 → 09:30 $31.00 +4.80; HYMC×43 yday $25.84 → 09:30 $25.73 -4.73; BTBT×7 yday $1.56 → 09:30 $1.55 -0.07; ENHA×7 yday $1.69 → 09:30 $1.65 -0.28; ORBS×14 yday $0.85 → 09:30 $0.85 +0.00; GORO×4 yday $3.57 → 09:30 $3.53 -0.16; QTRX×4 yday $2.76 → 09:30 $2.80 +0.16 | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 160 | $8.31 | $2.51 | $+143.82 | $1,353.52 | ▲ +143.82 after sell → book $9,791.90; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 159 | $6.82 | $2.50 | $-105.14 | $2,435.40 | ▼ -105.14 after sell → book $9,789.40; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 10 | $125.56 | $2.04 | $+119.24 | $3,688.96 | ▲ +119.24 after sell → book $9,787.36; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 110 | $10.72 | $2.35 | $-10.17 | $4,865.81 | ▼ -10.17 after sell → book $9,785.01; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 125 | $9.57 | $2.40 | $+8.99 | $6,059.66 | ▲ +8.99 after sell → book $9,782.61; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 141 | $9.34 | $2.45 | $+130.50 | $7,374.16 | ▲ +130.50 after sell → book $9,780.17; vs 09:30 mark -2.44 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 40 | $31.00 | $2.13 | $+67.76 | $8,612.03 | ▲ +67.76 after sell → book $9,778.04; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HYMC` | 43 | $25.73 | $2.14 | $-69.62 | $9,716.28 | ▼ -69.62 after sell → book $9,775.90; vs 09:30 mark -2.14 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 749 | $1.62 | $9.66 | — | $8,493.23 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1214.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 607 | $2.00 | $7.83 | — | $7,271.40 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1214.53 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 328 | $3.70 | $4.23 | — | $6,053.57 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1214.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 232 | $5.22 | $2.99 | — | $4,839.54 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1214.53 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 178 | $6.79 | $2.52 | — | $3,628.40 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1214.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 81 | $14.86 | $2.23 | — | $2,422.50 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1214.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 190 | $6.38 | $2.56 | — | $1,207.74 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1214.53 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3204 | $0.37 | $21.47 | — | $0.80 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $1214.53 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.80 | ▲ 09:30 equity $9,897.61 vs yday $9,897.61 (-0.00) | 09:30 open · cash $0.80 (unchanged overnight, no fees) · equity $9,897.61 vs prior close $9,897.61 (-0.00) because holdings re-marked: BTBT×7 yday $1.53 → 09:30 $1.53 +0.00; ENHA×7 yday $1.66 → 09:30 $1.66 +0.00; ORBS×14 yday $0.84 → 09:30 $0.84 +0.00; GORO×4 yday $3.56 → 09:30 $3.56 +0.00; QTRX×4 yday $2.80 → 09:30 $2.80 +0.00; BMEA×749 yday $1.61 → 09:30 $1.61 +0.00; NPWR×607 yday $2.02 → 09:30 $2.02 +0.00; PUSA×328 yday $3.91 → 09:30 $3.91 +0.00; ALVO×232 yday $5.25 → 09:30 $5.25 +0.00; CAPR×178 yday $7.19 → 09:30 $7.19 +0.00; ALIT×81 yday $14.87 → 09:30 $14.87 +0.00; ZURA×190 yday $6.50 → 09:30 $6.50 +0.00; SAFX×3204 yday $0.37 → 09:30 $0.37 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.80 | ▲ 09:30 equity $9,922.23 vs yday $9,722.40 (+199.83) | 09:30 open · cash $0.80 (unchanged overnight, no fees) · equity $9,922.23 vs prior close $9,722.40 (+199.83) because holdings re-marked: BTBT×7 yday $1.53 → 09:30 $1.53 +0.00; ENHA×7 yday $1.66 → 09:30 $1.63 -0.21; ORBS×14 yday $0.84 → 09:30 $0.80 -0.56; GORO×4 yday $3.56 → 09:30 $3.77 +0.84; QTRX×4 yday $2.80 → 09:30 $2.83 +0.12; BMEA×749 yday $1.61 → 09:30 $1.75 +104.86; NPWR×607 yday $2.02 → 09:30 $1.93 -54.63; PUSA×328 yday $3.91 → 09:30 $3.84 -22.96; ALVO×232 yday $5.25 → 09:30 $4.98 -62.64; CAPR×178 yday $7.19 → 09:30 $8.29 +195.80; ALIT×81 yday $14.87 → 09:30 $14.85 -1.62; ZURA×190 yday $6.50 → 09:30 $6.13 -70.30; SAFX×3204 yday $0.37 → 09:30 $0.35 -64.08 | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 7 | $1.53 | $0.15 | $-1.20 | $11.36 | ▼ -1.20 after sell → book $9,922.08; vs 09:30 mark -0.15 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ENHA` | 7 | $1.63 | $0.16 | $-0.86 | $22.61 | ▼ -0.86 after sell → book $9,921.92; vs 09:30 mark -0.16 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 14 | $0.80 | $0.17 | $-1.23 | $33.64 | ▼ -1.23 after sell → book $9,921.75; vs 09:30 mark -0.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 4 | $3.77 | $0.18 | $+2.32 | $48.54 | ▲ +2.32 after sell → book $9,921.57; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `QTRX` | 4 | $2.83 | $0.15 | $-1.40 | $59.71 | ▼ -1.40 after sell → book $9,921.42; vs 09:30 mark -0.15 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.71 | ▲ 09:30 equity $10,078.94 vs yday $10,055.56 (+23.38) | 09:30 open · cash $59.71 (unchanged overnight, no fees) · equity $10,078.94 vs prior close $10,055.56 (+23.38) because holdings re-marked: BMEA×749 yday $1.71 → 09:30 $1.74 +22.47; NPWR×607 yday $1.81 → 09:30 $1.83 +12.14; PUSA×328 yday $3.85 → 09:30 $3.86 +3.28; ALVO×232 yday $4.91 → 09:30 $4.88 -6.96; CAPR×178 yday $9.36 → 09:30 $9.19 -30.26; ALIT×81 yday $14.33 → 09:30 $14.54 +17.01; ZURA×190 yday $5.99 → 09:30 $6.02 +5.70; SAFX×3204 yday $0.39 → 09:30 $0.39 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 749 | $1.74 | $9.80 | $+70.42 | $1,353.17 | ▲ +70.42 after sell → book $10,069.14; vs 09:30 mark -9.80 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 607 | $1.83 | $7.94 | $-118.96 | $2,456.04 | ▼ -118.96 after sell → book $10,061.20; vs 09:30 mark -7.94 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 328 | $3.86 | $4.30 | $+43.95 | $3,717.83 | ▲ +43.95 after sell → book $10,056.91; vs 09:30 mark -4.29 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 232 | $4.88 | $3.04 | $-84.91 | $4,846.95 | ▼ -84.91 after sell → book $10,053.87; vs 09:30 mark -3.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 81 | $14.54 | $2.26 | $-30.41 | $6,022.43 | ▼ -30.41 after sell → book $10,051.61; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 190 | $6.02 | $2.60 | $-73.56 | $7,163.63 | ▼ -73.56 after sell → book $10,049.01; vs 09:30 mark -2.60 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3204 | $0.39 | $22.65 | $+19.96 | $8,390.54 | ▲ +19.96 after sell → book $10,026.36; vs 09:30 mark -22.65 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $7,230.92 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1198.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 70 | $16.95 | $2.20 | — | $6,042.22 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1198.65 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 64 | $18.50 | $2.18 | — | $4,856.04 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1198.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 868 | $1.38 | $11.20 | — | $3,647.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1198.65 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 35 | $33.78 | $2.10 | — | $2,462.61 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1198.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $1,265.40 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1198.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 75 | $15.94 | $2.21 | — | $67.68 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1198.65 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $67.68 | ▼ 09:30 equity $9,593.10 vs yday $9,972.03 (-378.93) | 09:30 open · cash $67.68 (unchanged overnight, no fees) · equity $9,593.10 vs prior close $9,972.03 (-378.93) because holdings re-marked: CAPR×178 yday $10.06 → 09:30 $9.44 -110.36; ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×70 yday $16.12 → 09:30 $15.44 -47.60; BZ×64 yday $18.00 → 09:30 $17.89 -7.04; LVWR×868 yday $1.36 → 09:30 $1.37 +8.68; SEDG×35 yday $33.51 → 09:30 $31.50 -70.35; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×75 yday $15.66 → 09:30 $14.32 -100.50 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 178 | $9.44 | $2.57 | $+466.61 | $1,745.43 | ▲ +466.61 after sell → book $9,590.53; vs 09:30 mark -2.57 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,745.43 | ▼ 09:30 equity $9,446.98 vs yday $9,546.16 (-99.18) | 09:30 open · cash $1,745.43 (unchanged overnight, no fees) · equity $9,446.98 vs prior close $9,546.16 (-99.18) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BHVN×70 yday $15.40 → 09:30 $15.45 +3.50; BZ×64 yday $17.90 → 09:30 $17.37 -33.92; LVWR×868 yday $1.34 → 09:30 $1.22 -104.16; SEDG×35 yday $31.27 → 09:30 $32.22 +33.25; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×75 yday $14.20 → 09:30 $15.05 +63.75 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,745.43 | ▼ 09:30 equity $9,340.95 vs yday $9,353.05 (-12.10) | 09:30 open · cash $1,745.43 (unchanged overnight, no fees) · equity $9,340.95 vs prior close $9,353.05 (-12.10) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BHVN×70 yday $15.45 → 09:30 $15.39 -4.20; BZ×64 yday $17.17 → 09:30 $17.29 +7.68; LVWR×868 yday $1.18 → 09:30 $1.19 +8.68; SEDG×35 yday $31.80 → 09:30 $31.87 +2.45; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×75 yday $14.80 → 09:30 $14.75 -3.75 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $142.00 | $2.03 | $-25.65 | $2,879.40 | ▼ -25.65 after sell → book $9,338.92; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 70 | $15.39 | $2.22 | $-113.62 | $3,954.48 | ▼ -113.62 after sell → book $9,336.70; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 64 | $17.29 | $2.20 | $-81.82 | $5,058.84 | ▼ -81.82 after sell → book $9,334.50; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 868 | $1.19 | $11.35 | $-187.47 | $6,080.40 | ▼ -187.47 after sell → book $9,323.14; vs 09:30 mark -11.36 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 35 | $31.87 | $2.12 | $-71.06 | $7,193.74 | ▼ -71.06 after sell → book $9,321.03; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $8,212.75 | ▼ -178.21 after sell → book $9,319.00; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 75 | $14.75 | $2.24 | $-93.70 | $9,316.76 | ▼ -93.70 after sell → book $9,316.76; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,316.76 | ▲ 09:30 equity $9,316.76 vs yday $9,316.76 (-0.00) | 09:30 open · cash $9,316.76 · no holdings · equity $9,316.76 vs prior close $9,316.76 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 954 | $1.22 | $12.31 | — | $8,140.57 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1164.59 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 63 | $18.40 | $2.18 | — | $6,979.19 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1164.59 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 74 | $15.70 | $2.21 | — | $5,815.18 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1164.59 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $4,651.26 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1164.59 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 313 | $3.72 | $4.04 | — | $3,482.86 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1164.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 79 | $14.70 | $2.23 | — | $2,319.33 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1164.59 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 20 | $56.78 | $2.05 | — | $1,181.68 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1164.59 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 539 | $2.16 | $6.95 | — | $10.49 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1164.59 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.49 | ▲ 09:30 equity $9,844.86 vs yday $9,723.41 (+121.45) | 09:30 open · cash $10.49 (unchanged overnight, no fees) · equity $9,844.86 vs prior close $9,723.41 (+121.45) because holdings re-marked: GPRO×954 yday $1.69 → 09:30 $1.78 +85.86; FRVO×63 yday $17.98 → 09:30 $18.27 +18.27; CRK×74 yday $15.54 → 09:30 $15.45 -6.66; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; CTMX×313 yday $3.72 → 09:30 $3.73 +3.13; SLN×79 yday $14.79 → 09:30 $14.85 +4.74; EIX×20 yday $55.19 → 09:30 $55.42 +4.60; CRDL×539 yday $2.17 → 09:30 $2.18 +5.39 | — |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 33.64 < 1 share @ 39.85 |
| 2026-08-17 | `CELC` | cash | leftover split 33.64 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `FCEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `BW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `FCEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `BW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HYMC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 12.53 < 1 share @ 623.26 |
| 2026-08-21 | `QDEL` | cash | leftover split 12.53 < 1 share @ 14.96 |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HYMC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ENHA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `QTRX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `BAK` | cash | leftover split 1.50 < 1 share @ 1.95 |
| 2026-09-04 | `EOSE` | cash | leftover split 1.50 < 1 share @ 3.57 |
| 2026-09-04 | `SLBT` | cash | leftover split 1.50 < 1 share @ 3.07 |
| 2026-09-04 | `DELL` | cash | leftover split 1.50 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 1.50 < 1 share @ 29.15 |
| 2026-09-04 | `CCOI` | cash | leftover split 1.50 < 1 share @ 10.22 |
| 2026-09-04 | `SION` | cash | leftover split 1.50 < 1 share @ 7.31 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 954 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1164.59 |
| `FRVO` | 63 | 2026-09-03 @ $18.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1164.59 |
| `CRK` | 74 | 2026-09-03 @ $15.70 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1164.59 |
| `MMED` | 51 | 2026-09-03 @ $22.78 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1164.59 |
| `CTMX` | 313 | 2026-09-03 @ $3.72 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1164.59 |
| `SLN` | 79 | 2026-09-03 @ $14.70 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1164.59 |
| `EIX` | 20 | 2026-09-03 @ $56.78 | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1164.59 |
| `CRDL` | 539 | 2026-09-03 @ $2.16 | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1164.59 |
