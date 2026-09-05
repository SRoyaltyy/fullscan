# Factor mine action — `probable_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `probable` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-19.05%** ($8,095) · signal-only (no cash/fees) was -5.25%. Starts YES **6/17**. Fills 96 · skips 228 · realized $-1793.65.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `probable` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $159.52.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | — | $269.08 | $9,985.46 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $269.08 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28 | $10,059.20 | +73.74 | ABX, FCEL, VERA, BW, OCC, ALM | — | $104.70 | $9,954.02 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | 09:30 open · cash $269.08 (unchanged overnight, no fees) · equity $10,059.20 vs prior close $9,985.46 (+73.74) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; WDC×2 yday $508.80 → 09:30 $525.53 +33.46; FOSL×221 yday $5.57 → 09:30 $5.50 -15.47; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRS×370 yday $3.43 → 09:30 $3.40 -12.95; ALGM×28 yday $44.39 → 09:30 $45.32 +26.04 |
| 2026-08-18 | -6.20 | $104.70 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | $9,758.08 | -195.94 | — | — | $104.70 | $9,500.71 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | 09:30 open · cash $104.70 (unchanged overnight, no fees) · equity $9,758.08 vs prior close $9,954.02 (-195.94) because holdings re-marked: ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; WWW×60 yday $19.83 → 09:30 $19.95 +7.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; WDC×2 yday $536.01 → 09:30 $496.07 -79.88; FOSL×221 yday $5.74 → 09:30 $5.78 +8.84; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; AIRS×370 yday $3.08 → 09:30 $3.01 -27.75; ALGM×28 yday $44.25 → 09:30 $42.54 -47.88; ABX×3 yday $9.12 → 09:30 $9.03 -0.27; FCEL×1 yday $22.36 → 09:30 $21.18 -1.18; VERA×1 yday $31.63 → 09:30 $31.31 -0.32; BW×3 yday $9.92 → 09:30 $9.60 -0.96; OCC×1 yday $17.12 → 09:30 $16.20 -0.92; ALM×2 yday $16.36 → 09:30 $15.78 -1.16 |
| 2026-08-19 | -7.20 | $104.70 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | $9,522.41 | +21.70 | — | — | $104.70 | $9,390.53 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | 09:30 open · cash $104.70 (unchanged overnight, no fees) · equity $9,522.41 vs prior close $9,500.71 (+21.70) because holdings re-marked: ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; WWW×60 yday $19.99 → 09:30 $20.08 +5.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; WDC×2 yday $496.16 → 09:30 $494.28 -3.76; FOSL×221 yday $5.50 → 09:30 $5.54 +8.84; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; AIRS×370 yday $2.69 → 09:30 $2.71 +5.55; ALGM×28 yday $39.39 → 09:30 $40.00 +17.08; ABX×3 yday $9.01 → 09:30 $9.08 +0.21; FCEL×1 yday $21.70 → 09:30 $21.48 -0.22; VERA×1 yday $32.28 → 09:30 $32.88 +0.60; BW×3 yday $9.14 → 09:30 $9.14 +0.00; OCC×1 yday $16.20 → 09:30 $16.21 +0.01; ALM×2 yday $15.60 → 09:30 $16.05 +0.90 |
| 2026-08-20 | +1.12 | $104.70 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2 | $9,303.97 | -86.56 | MRVI, DNA, EXK, SCZM, NG | — | $60.81 | $9,148.49 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2, MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1 | 09:30 open · cash $104.70 (unchanged overnight, no fees) · equity $9,303.97 vs prior close $9,390.53 (-86.56) because holdings re-marked: ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; WWW×60 yday $20.85 → 09:30 $20.15 -42.00; HYLN×299 yday $3.67 → 09:30 $3.61 -17.94; WDC×2 yday $462.09 → 09:30 $463.39 +2.60; FOSL×221 yday $5.69 → 09:30 $5.60 -19.89; ADUR×75 yday $15.39 → 09:30 $15.55 +12.00; AIRS×370 yday $2.81 → 09:30 $2.79 -7.40; ALGM×28 yday $38.35 → 09:30 $38.21 -3.92; ABX×3 yday $9.15 → 09:30 $9.13 -0.06; FCEL×1 yday $20.30 → 09:30 $20.21 -0.09; VERA×1 yday $32.27 → 09:30 $32.30 +0.02; BW×3 yday $9.11 → 09:30 $9.05 -0.18; OCC×1 yday $14.36 → 09:30 $14.10 -0.26; ALM×2 yday $16.18 → 09:30 $15.81 -0.74 |
| 2026-08-21 | +3.25 | $60.81 | ANGX×290, WWW×60, HYLN×299, WDC×2, FOSL×221, ADUR×75, AIRS×370, ALGM×28, ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2, MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1 | $9,246.29 | +97.80 | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX | ANGX, WWW, HYLN, WDC, FOSL, ADUR, AIRS, ALGM | $1.63 | $9,115.88 | ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2, MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413 | 09:30 open · cash $60.81 (unchanged overnight, no fees) · equity $9,246.29 vs prior close $9,148.49 (+97.80) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.43 +17.40; WWW×60 yday $20.45 → 09:30 $20.32 -7.80; HYLN×299 yday $3.37 → 09:30 $3.42 +14.95; WDC×2 yday $469.05 → 09:30 $477.27 +16.44; FOSL×221 yday $5.58 → 09:30 $5.65 +15.47; ADUR×75 yday $15.85 → 09:30 $16.00 +11.25; AIRS×370 yday $2.67 → 09:30 $2.71 +14.80; ALGM×28 yday $37.19 → 09:30 $37.62 +12.18; ABX×3 yday $9.16 → 09:30 $9.13 -0.09; FCEL×1 yday $18.36 → 09:30 $19.01 +0.65; VERA×1 yday $31.26 → 09:30 $31.42 +0.16; BW×3 yday $8.43 → 09:30 $8.56 +0.39; OCC×1 yday $14.12 → 09:30 $14.20 +0.08; ALM×2 yday $17.69 → 09:30 $18.00 +0.62; MRVI×1 yday $8.26 → 09:30 $8.20 -0.06; DNA×1 yday $6.96 → 09:30 $7.09 +0.13; EXK×1 yday $10.97 → 09:30 $11.34 +0.37; SCZM×1 yday $9.76 → 09:30 $10.26 +0.50; NG×1 yday $8.66 → 09:30 $9.02 +0.36 |
| 2026-08-24 | -5.17 | $1.63 | ABX×3, FCEL×1, VERA×1, BW×3, OCC×1, ALM×2, MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413 | $9,170.91 | +55.03 | — | ABX, FCEL, VERA, BW, OCC, ALM | $155.78 | $9,112.59 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413 | 09:30 open · cash $1.63 (unchanged overnight, no fees) · equity $9,170.91 vs prior close $9,115.88 (+55.03) because holdings re-marked: ABX×3 yday $9.66 → 09:30 $9.90 +0.72; FCEL×1 yday $19.54 → 09:30 $18.51 -1.03; VERA×1 yday $32.27 → 09:30 $32.25 -0.02; BW×3 yday $8.28 → 09:30 $8.14 -0.42; OCC×1 yday $13.85 → 09:30 $13.60 -0.25; ALM×2 yday $18.51 → 09:30 $18.69 +0.36; MRVI×1 yday $8.70 → 09:30 $8.59 -0.11; DNA×1 yday $7.40 → 09:30 $7.26 -0.14; EXK×1 yday $10.62 → 09:30 $11.01 +0.39; SCZM×1 yday $9.68 → 09:30 $9.82 +0.14; NG×1 yday $8.72 → 09:30 $8.89 +0.17; BTBT×776 yday $1.53 → 09:30 $1.55 +15.52; ENHA×753 yday $1.72 → 09:30 $1.74 +15.06; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×86 yday $14.74 → 09:30 $14.71 -2.58; ORBS×1491 yday $0.88 → 09:30 $0.89 +14.91; GORO×414 yday $3.19 → 09:30 $3.20 +4.14; QTRX×413 yday $2.99 → 09:30 $2.98 -4.13 |
| 2026-08-25 | +1.80 | $155.78 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413 | $9,074.38 | -38.21 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | — | $15.72 | $9,065.78 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | 09:30 open · cash $155.78 (unchanged overnight, no fees) · equity $9,074.38 vs prior close $9,112.59 (-38.21) because holdings re-marked: MRVI×1 yday $8.26 → 09:30 $8.31 +0.05; DNA×1 yday $6.98 → 09:30 $6.82 -0.16; EXK×1 yday $10.74 → 09:30 $10.72 -0.02; SCZM×1 yday $9.53 → 09:30 $9.57 +0.04; NG×1 yday $9.24 → 09:30 $9.34 +0.10; BTBT×776 yday $1.56 → 09:30 $1.55 -7.76; ENHA×753 yday $1.69 → 09:30 $1.65 -30.12; DE×2 yday $654.38 → 09:30 $648.64 -11.48; QDEL×86 yday $14.36 → 09:30 $14.49 +11.18; ORBS×1491 yday $0.85 → 09:30 $0.85 +0.00; GORO×414 yday $3.57 → 09:30 $3.53 -16.56; QTRX×413 yday $2.76 → 09:30 $2.80 +16.52 |
| 2026-08-26 | +2.02 | $15.72 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | $9,065.78 | -0.00 | — | — | $15.72 | $9,072.74 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | 09:30 open · cash $15.72 (unchanged overnight, no fees) · equity $9,065.78 vs prior close $9,065.78 (-0.00) because holdings re-marked: MRVI×1 yday $8.49 → 09:30 $8.49 +0.00; DNA×1 yday $6.89 → 09:30 $6.89 +0.00; EXK×1 yday $10.67 → 09:30 $10.67 +0.00; SCZM×1 yday $9.57 → 09:30 $9.57 +0.00; NG×1 yday $9.35 → 09:30 $9.35 +0.00; BTBT×776 yday $1.53 → 09:30 $1.53 +0.00; ENHA×753 yday $1.66 → 09:30 $1.66 +0.00; DE×2 yday $649.11 → 09:30 $649.11 +0.00; QDEL×86 yday $14.49 → 09:30 $14.49 +0.00; ORBS×1491 yday $0.84 → 09:30 $0.84 +0.00; GORO×414 yday $3.56 → 09:30 $3.56 +0.00; QTRX×413 yday $2.80 → 09:30 $2.80 +0.00; BMEA×12 yday $1.61 → 09:30 $1.61 +0.00; NPWR×9 yday $2.02 → 09:30 $2.02 +0.00; PUSA×5 yday $3.91 → 09:30 $3.91 +0.00; ALVO×3 yday $5.25 → 09:30 $5.25 +0.00; CAPR×2 yday $7.19 → 09:30 $7.19 +0.00; ALIT×1 yday $14.87 → 09:30 $14.87 +0.00; ZURA×3 yday $6.50 → 09:30 $6.50 +0.00; SAFX×52 yday $0.37 → 09:30 $0.37 +0.00 |
| 2026-08-27 | — | $15.72 | MRVI×1, DNA×1, EXK×1, SCZM×1, NG×1, BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | $9,101.49 | +28.75 | — | MRVI, DNA, EXK, SCZM, NG | $61.30 | $8,951.10 | BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | 09:30 open · cash $15.72 (unchanged overnight, no fees) · equity $9,101.49 vs prior close $9,072.74 (+28.75) because holdings re-marked: MRVI×1 yday $8.49 → 09:30 $8.85 +0.36; DNA×1 yday $6.89 → 09:30 $7.33 +0.44; EXK×1 yday $10.67 → 09:30 $10.82 +0.15; SCZM×1 yday $9.57 → 09:30 $9.61 +0.04; NG×1 yday $9.35 → 09:30 $9.55 +0.20; BTBT×776 yday $1.53 → 09:30 $1.53 +0.00; ENHA×753 yday $1.66 → 09:30 $1.63 -22.59; DE×2 yday $649.11 → 09:30 $632.15 -33.92; QDEL×86 yday $14.49 → 09:30 $15.09 +51.60; ORBS×1491 yday $0.84 → 09:30 $0.80 -59.64; GORO×414 yday $3.56 → 09:30 $3.77 +86.94; QTRX×413 yday $2.80 → 09:30 $2.83 +12.39; BMEA×12 yday $1.61 → 09:30 $1.75 +1.68; NPWR×9 yday $2.02 → 09:30 $1.93 -0.81; PUSA×5 yday $3.91 → 09:30 $3.84 -0.35; ALVO×3 yday $5.25 → 09:30 $4.98 -0.81; CAPR×2 yday $7.19 → 09:30 $8.29 +2.20; ALIT×1 yday $14.87 → 09:30 $14.85 -0.02; ZURA×3 yday $6.50 → 09:30 $6.13 -1.11; SAFX×52 yday $0.37 → 09:30 $0.35 -1.04 |
| 2026-08-28 | +0.75 | $61.30 | BTBT×776, ENHA×753, DE×2, QDEL×86, ORBS×1491, GORO×414, QTRX×413, BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52 | $9,020.83 | +69.73 | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | BTBT, ENHA, DE, QDEL, ORBS, GORO, QTRX | $167.51 | $8,750.40 | BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52, ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | 09:30 open · cash $61.30 (unchanged overnight, no fees) · equity $9,020.83 vs prior close $8,951.10 (+69.73) because holdings re-marked: BTBT×776 yday $1.56 → 09:30 $1.59 +23.28; ENHA×753 yday $1.61 → 09:30 $1.64 +22.59; DE×2 yday $634.54 → 09:30 $628.82 -11.44; QDEL×86 yday $14.91 → 09:30 $14.92 +0.86; ORBS×1491 yday $0.80 → 09:30 $0.82 +29.82; GORO×414 yday $3.56 → 09:30 $3.59 +12.42; QTRX×413 yday $2.68 → 09:30 $2.66 -8.26; BMEA×12 yday $1.71 → 09:30 $1.74 +0.36; NPWR×9 yday $1.81 → 09:30 $1.83 +0.18; PUSA×5 yday $3.85 → 09:30 $3.86 +0.05; ALVO×3 yday $4.91 → 09:30 $4.88 -0.09; CAPR×2 yday $9.36 → 09:30 $9.19 -0.34; ALIT×1 yday $14.33 → 09:30 $14.54 +0.21; ZURA×3 yday $5.99 → 09:30 $6.02 +0.09; SAFX×52 yday $0.39 → 09:30 $0.39 +0.00 |
| 2026-08-31 | -5.85 | $167.51 | BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52, ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | $8,467.86 | -282.54 | — | — | $167.51 | $8,420.76 | BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52, ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | 09:30 open · cash $167.51 (unchanged overnight, no fees) · equity $8,467.86 vs prior close $8,750.40 (-282.54) because holdings re-marked: BMEA×12 yday $1.68 → 09:30 $1.71 +0.36; NPWR×9 yday $1.89 → 09:30 $1.83 -0.54; PUSA×5 yday $3.79 → 09:30 $3.72 -0.35; ALVO×3 yday $4.88 → 09:30 $4.98 +0.30; CAPR×2 yday $10.06 → 09:30 $9.44 -1.24; ALIT×1 yday $14.21 → 09:30 $14.30 +0.09; ZURA×3 yday $5.85 → 09:30 $5.51 -1.02; SAFX×52 yday $0.37 → 09:30 $0.38 +0.52; ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×74 yday $16.12 → 09:30 $15.44 -50.32; BZ×68 yday $18.00 → 09:30 $17.89 -7.48; LVWR×913 yday $1.36 → 09:30 $1.37 +9.13; SEDG×37 yday $33.51 → 09:30 $31.50 -74.37; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×79 yday $15.66 → 09:30 $14.32 -105.86 |
| 2026-09-01 | -6.30 | $167.51 | BMEA×12, NPWR×9, PUSA×5, ALVO×3, CAPR×2, ALIT×1, ZURA×3, SAFX×52, ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | $8,322.69 | -98.07 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | $308.47 | $8,222.47 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | 09:30 open · cash $167.51 (unchanged overnight, no fees) · equity $8,322.69 vs prior close $8,420.76 (-98.07) because holdings re-marked: BMEA×12 yday $1.71 → 09:30 $1.65 -0.72; NPWR×9 yday $1.82 → 09:30 $1.78 -0.36; PUSA×5 yday $3.80 → 09:30 $3.93 +0.65; ALVO×3 yday $4.96 → 09:30 $5.24 +0.84; CAPR×2 yday $9.36 → 09:30 $10.43 +2.14; ALIT×1 yday $14.02 → 09:30 $14.72 +0.70; ZURA×3 yday $5.64 → 09:30 $5.60 -0.12; SAFX×52 yday $0.37 → 09:30 $0.37 +0.00; ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BHVN×74 yday $15.40 → 09:30 $15.45 +3.70; BZ×68 yday $17.90 → 09:30 $17.37 -36.04; LVWR×913 yday $1.34 → 09:30 $1.22 -109.56; SEDG×37 yday $31.27 → 09:30 $32.22 +35.15; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×79 yday $14.20 → 09:30 $15.05 +67.15 |
| 2026-09-02 | -3.83 | $308.47 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | $8,211.00 | -11.47 | — | — | $308.47 | $8,206.29 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | 09:30 open · cash $308.47 (unchanged overnight, no fees) · equity $8,211.00 vs prior close $8,222.47 (-11.47) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BHVN×74 yday $15.45 → 09:30 $15.39 -4.44; BZ×68 yday $17.17 → 09:30 $17.29 +8.16; LVWR×913 yday $1.18 → 09:30 $1.19 +9.13; SEDG×37 yday $31.80 → 09:30 $31.87 +2.59; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×79 yday $14.80 → 09:30 $14.75 -3.95 |
| 2026-09-03 | -0.90 | $308.47 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79 | $8,239.08 | +32.79 | GPRO, FRVO, CRK, MMED, CTMX, SLN, CRDL | — | $73.83 | $8,258.49 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79, GPRO×31, FRVO×2, CRK×2, MMED×1, CTMX×10, SLN×2, CRDL×17 | 09:30 open · cash $308.47 (unchanged overnight, no fees) · equity $8,239.08 vs prior close $8,206.29 (+32.79) because holdings re-marked: ANF×8 yday $140.68 → 09:30 $139.65 -8.24; BHVN×74 yday $15.74 → 09:30 $15.97 +17.02; BZ×68 yday $17.55 → 09:30 $17.65 +6.80; LVWR×913 yday $1.14 → 09:30 $1.17 +27.39; SEDG×37 yday $32.49 → 09:30 $32.42 -2.59; SMTC×8 yday $132.27 → 09:30 $133.00 +5.84; GRRR×79 yday $14.09 → 09:30 $13.92 -13.43 |
| 2026-09-04 | — | $73.83 | ANF×8, BHVN×74, BZ×68, LVWR×913, SEDG×37, SMTC×8, GRRR×79, GPRO×31, FRVO×2, CRK×2, MMED×1, CTMX×10, SLN×2, CRDL×17 | $8,247.09 | -11.40 | BAK, EOSE, SLBT, DELL, MLYS, CCOI, SION | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | $159.52 | $8,094.57 | GPRO×31, FRVO×2, CRK×2, MMED×1, CTMX×10, SLN×2, CRDL×17, BAK×584, EOSE×318, SLBT×370, DELL×2, MLYS×39, CCOI×111, SION×155 | 09:30 open · cash $73.83 (unchanged overnight, no fees) · equity $8,247.09 vs prior close $8,258.49 (-11.40) because holdings re-marked: ANF×8 yday $136.60 → 09:30 $137.70 +8.80; BHVN×74 yday $15.69 → 09:30 $15.89 +14.80; BZ×68 yday $17.30 → 09:30 $17.31 +0.68; LVWR×913 yday $1.20 → 09:30 $1.17 -27.39; SEDG×37 yday $33.98 → 09:30 $33.69 -10.73; SMTC×8 yday $133.85 → 09:30 $133.10 -6.00; GRRR×79 yday $13.72 → 09:30 $13.78 +4.74; GPRO×31 yday $1.69 → 09:30 $1.78 +2.79; FRVO×2 yday $17.98 → 09:30 $18.27 +0.58; CRK×2 yday $15.54 → 09:30 $15.45 -0.18; MMED×1 yday $23.76 → 09:30 $23.88 +0.12; CTMX×10 yday $3.72 → 09:30 $3.73 +0.10; SLN×2 yday $14.79 → 09:30 $14.85 +0.12; CRDL×17 yday $2.17 → 09:30 $2.18 +0.17 |

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
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $104.70 | ▼ 09:30 equity $9,303.97 vs yday $9,390.53 (-86.56) | 09:30 open · cash $104.70 (unchanged overnight, no fees) · equity $9,303.97 vs prior close $9,390.53 (-86.56) because holdings re-marked: ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; WWW×60 yday $20.85 → 09:30 $20.15 -42.00; HYLN×299 yday $3.67 → 09:30 $3.61 -17.94; WDC×2 yday $462.09 → 09:30 $463.39 +2.60; FOSL×221 yday $5.69 → 09:30 $5.60 -19.89; ADUR×75 yday $15.39 → 09:30 $15.55 +12.00; AIRS×370 yday $2.81 → 09:30 $2.79 -7.40; ALGM×28 yday $38.35 → 09:30 $38.21 -3.92; ABX×3 yday $9.15 → 09:30 $9.13 -0.06; FCEL×1 yday $20.30 → 09:30 $20.21 -0.09; VERA×1 yday $32.27 → 09:30 $32.30 +0.02; BW×3 yday $9.11 → 09:30 $9.05 -0.18; OCC×1 yday $14.36 → 09:30 $14.10 -0.26; ALM×2 yday $16.18 → 09:30 $15.81 -0.74 | — |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 1 | $7.38 | $0.08 | — | $97.25 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $13.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 1 | $7.45 | $0.08 | — | $89.72 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $13.09 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 1 | $10.77 | $0.11 | — | $78.84 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $13.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 1 | $9.46 | $0.10 | — | $69.28 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $13.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 1 | $8.38 | $0.09 | — | $60.81 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $13.09 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $60.81 | ▲ 09:30 equity $9,246.29 vs yday $9,148.49 (+97.80) | 09:30 open · cash $60.81 (unchanged overnight, no fees) · equity $9,246.29 vs prior close $9,148.49 (+97.80) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.43 +17.40; WWW×60 yday $20.45 → 09:30 $20.32 -7.80; HYLN×299 yday $3.37 → 09:30 $3.42 +14.95; WDC×2 yday $469.05 → 09:30 $477.27 +16.44; FOSL×221 yday $5.58 → 09:30 $5.65 +15.47; ADUR×75 yday $15.85 → 09:30 $16.00 +11.25; AIRS×370 yday $2.67 → 09:30 $2.71 +14.80; ALGM×28 yday $37.19 → 09:30 $37.62 +12.18; ABX×3 yday $9.16 → 09:30 $9.13 -0.09; FCEL×1 yday $18.36 → 09:30 $19.01 +0.65; VERA×1 yday $31.26 → 09:30 $31.42 +0.16; BW×3 yday $8.43 → 09:30 $8.56 +0.39; OCC×1 yday $14.12 → 09:30 $14.20 +0.08; ALM×2 yday $17.69 → 09:30 $18.00 +0.62; MRVI×1 yday $8.26 → 09:30 $8.20 -0.06; DNA×1 yday $6.96 → 09:30 $7.09 +0.13; EXK×1 yday $10.97 → 09:30 $11.34 +0.37; SCZM×1 yday $9.76 → 09:30 $10.26 +0.50; NG×1 yday $8.66 → 09:30 $9.02 +0.36 | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $1,341.71 | ▲ +27.26 after sell → book $9,242.49; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `WWW` | 60 | $20.32 | $2.19 | $-21.16 | $2,558.72 | ▼ -21.16 after sell → book $9,240.30; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 299 | $3.42 | $3.92 | $-235.01 | $3,577.39 | ▼ -235.01 after sell → book $9,236.39; vs 09:30 mark -3.91 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `WDC` | 2 | $477.27 | $2.02 | $-56.47 | $4,529.91 | ▼ -56.47 after sell → book $9,234.37; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `FOSL` | 221 | $5.65 | $2.90 | $-3.54 | $5,775.66 | ▼ -3.54 after sell → book $9,231.47; vs 09:30 mark -2.90 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $6,973.43 | ▼ -41.95 after sell → book $9,229.24; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AIRS` | 370 | $2.71 | $4.84 | $-253.82 | $7,971.28 | ▼ -253.82 after sell → book $9,224.39; vs 09:30 mark -4.85 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ALGM` | 28 | $37.62 | $2.09 | $-184.35 | $9,022.69 | ▼ -184.35 after sell → book $9,222.30; vs 09:30 mark -2.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 776 | $1.66 | $10.01 | — | $7,724.52 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1288.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 753 | $1.71 | $9.71 | — | $6,427.17 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1288.96 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $5,178.66 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1288.96 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 86 | $14.96 | $2.25 | — | $3,889.85 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1288.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1491 | $0.86 | $17.36 | — | $2,584.27 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1288.96 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 414 | $3.11 | $5.34 | — | $1,291.39 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+7.1; leftover $1288.96 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QTRX` | 413 | $3.11 | $5.33 | — | $1.63 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.1; leftover $1288.96 | join🟡 sector🟢 gen🟢 news🔴 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.63 | ▲ 09:30 equity $9,170.91 vs yday $9,115.88 (+55.03) | 09:30 open · cash $1.63 (unchanged overnight, no fees) · equity $9,170.91 vs prior close $9,115.88 (+55.03) because holdings re-marked: ABX×3 yday $9.66 → 09:30 $9.90 +0.72; FCEL×1 yday $19.54 → 09:30 $18.51 -1.03; VERA×1 yday $32.27 → 09:30 $32.25 -0.02; BW×3 yday $8.28 → 09:30 $8.14 -0.42; OCC×1 yday $13.85 → 09:30 $13.60 -0.25; ALM×2 yday $18.51 → 09:30 $18.69 +0.36; MRVI×1 yday $8.70 → 09:30 $8.59 -0.11; DNA×1 yday $7.40 → 09:30 $7.26 -0.14; EXK×1 yday $10.62 → 09:30 $11.01 +0.39; SCZM×1 yday $9.68 → 09:30 $9.82 +0.14; NG×1 yday $8.72 → 09:30 $8.89 +0.17; BTBT×776 yday $1.53 → 09:30 $1.55 +15.52; ENHA×753 yday $1.72 → 09:30 $1.74 +15.06; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×86 yday $14.74 → 09:30 $14.71 -2.58; ORBS×1491 yday $0.88 → 09:30 $0.89 +14.91; GORO×414 yday $3.19 → 09:30 $3.20 +4.14; QTRX×413 yday $2.99 → 09:30 $2.98 -4.13 | — |
| 2026-08-24 09:30 ET | **SELL** | `ABX` | 3 | $9.90 | $0.33 | $+1.73 | $31.01 | ▲ +1.73 after sell → book $9,170.59; vs 09:30 mark -0.32 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `FCEL` | 1 | $18.51 | $0.21 | $-4.29 | $49.31 | ▼ -4.29 after sell → book $9,170.38; vs 09:30 mark -0.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `VERA` | 1 | $32.25 | $0.35 | $+0.29 | $81.21 | ▲ +0.29 after sell → book $9,170.03; vs 09:30 mark -0.35 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `BW` | 3 | $8.14 | $0.27 | $-7.22 | $105.36 | ▼ -7.22 after sell → book $9,169.76; vs 09:30 mark -0.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `OCC` | 1 | $13.60 | $0.16 | $-4.98 | $118.80 | ▼ -4.98 after sell → book $9,169.60; vs 09:30 mark -0.16 | dropped from list after 5 sess (min 5) | — |
| 2026-08-24 09:30 ET | **SELL** | `ALM` | 2 | $18.69 | $0.40 | $+4.25 | $155.78 | ▲ +4.25 after sell → book $9,169.20; vs 09:30 mark -0.40 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 vol🟢 buy🟢 |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $155.78 | ▼ 09:30 equity $9,074.38 vs yday $9,112.59 (-38.21) | 09:30 open · cash $155.78 (unchanged overnight, no fees) · equity $9,074.38 vs prior close $9,112.59 (-38.21) because holdings re-marked: MRVI×1 yday $8.26 → 09:30 $8.31 +0.05; DNA×1 yday $6.98 → 09:30 $6.82 -0.16; EXK×1 yday $10.74 → 09:30 $10.72 -0.02; SCZM×1 yday $9.53 → 09:30 $9.57 +0.04; NG×1 yday $9.24 → 09:30 $9.34 +0.10; BTBT×776 yday $1.56 → 09:30 $1.55 -7.76; ENHA×753 yday $1.69 → 09:30 $1.65 -30.12; DE×2 yday $654.38 → 09:30 $648.64 -11.48; QDEL×86 yday $14.36 → 09:30 $14.49 +11.18; ORBS×1491 yday $0.85 → 09:30 $0.85 +0.00; GORO×414 yday $3.57 → 09:30 $3.53 -16.56; QTRX×413 yday $2.76 → 09:30 $2.80 +16.52 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 12 | $1.62 | $0.23 | — | $136.11 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $19.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 9 | $2.00 | $0.21 | — | $117.90 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $19.47 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 5 | $3.70 | $0.20 | — | $99.20 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $19.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 3 | $5.22 | $0.17 | — | $83.38 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $19.47 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 2 | $6.79 | $0.14 | — | $69.66 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $19.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 1 | $14.86 | $0.15 | — | $54.64 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $19.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 3 | $6.38 | $0.20 | — | $35.30 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $19.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 52 | $0.37 | $0.35 | — | $15.72 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $19.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.72 | ▲ 09:30 equity $9,065.78 vs yday $9,065.78 (-0.00) | 09:30 open · cash $15.72 (unchanged overnight, no fees) · equity $9,065.78 vs prior close $9,065.78 (-0.00) because holdings re-marked: MRVI×1 yday $8.49 → 09:30 $8.49 +0.00; DNA×1 yday $6.89 → 09:30 $6.89 +0.00; EXK×1 yday $10.67 → 09:30 $10.67 +0.00; SCZM×1 yday $9.57 → 09:30 $9.57 +0.00; NG×1 yday $9.35 → 09:30 $9.35 +0.00; BTBT×776 yday $1.53 → 09:30 $1.53 +0.00; ENHA×753 yday $1.66 → 09:30 $1.66 +0.00; DE×2 yday $649.11 → 09:30 $649.11 +0.00; QDEL×86 yday $14.49 → 09:30 $14.49 +0.00; ORBS×1491 yday $0.84 → 09:30 $0.84 +0.00; GORO×414 yday $3.56 → 09:30 $3.56 +0.00; QTRX×413 yday $2.80 → 09:30 $2.80 +0.00; BMEA×12 yday $1.61 → 09:30 $1.61 +0.00; NPWR×9 yday $2.02 → 09:30 $2.02 +0.00; PUSA×5 yday $3.91 → 09:30 $3.91 +0.00; ALVO×3 yday $5.25 → 09:30 $5.25 +0.00; CAPR×2 yday $7.19 → 09:30 $7.19 +0.00; ALIT×1 yday $14.87 → 09:30 $14.87 +0.00; ZURA×3 yday $6.50 → 09:30 $6.50 +0.00; SAFX×52 yday $0.37 → 09:30 $0.37 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $15.72 | ▲ 09:30 equity $9,101.49 vs yday $9,072.74 (+28.75) | 09:30 open · cash $15.72 (unchanged overnight, no fees) · equity $9,101.49 vs prior close $9,072.74 (+28.75) because holdings re-marked: MRVI×1 yday $8.49 → 09:30 $8.85 +0.36; DNA×1 yday $6.89 → 09:30 $7.33 +0.44; EXK×1 yday $10.67 → 09:30 $10.82 +0.15; SCZM×1 yday $9.57 → 09:30 $9.61 +0.04; NG×1 yday $9.35 → 09:30 $9.55 +0.20; BTBT×776 yday $1.53 → 09:30 $1.53 +0.00; ENHA×753 yday $1.66 → 09:30 $1.63 -22.59; DE×2 yday $649.11 → 09:30 $632.15 -33.92; QDEL×86 yday $14.49 → 09:30 $15.09 +51.60; ORBS×1491 yday $0.84 → 09:30 $0.80 -59.64; GORO×414 yday $3.56 → 09:30 $3.77 +86.94; QTRX×413 yday $2.80 → 09:30 $2.83 +12.39; BMEA×12 yday $1.61 → 09:30 $1.75 +1.68; NPWR×9 yday $2.02 → 09:30 $1.93 -0.81; PUSA×5 yday $3.91 → 09:30 $3.84 -0.35; ALVO×3 yday $5.25 → 09:30 $4.98 -0.81; CAPR×2 yday $7.19 → 09:30 $8.29 +2.20; ALIT×1 yday $14.87 → 09:30 $14.85 -0.02; ZURA×3 yday $6.50 → 09:30 $6.13 -1.11; SAFX×52 yday $0.37 → 09:30 $0.35 -1.04 | — |
| 2026-08-27 09:30 ET | **SELL** | `MRVI` | 1 | $8.85 | $0.11 | $+1.28 | $24.45 | ▲ +1.28 after sell → book $9,101.37; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `DNA` | 1 | $7.33 | $0.10 | $-0.29 | $31.69 | ▼ -0.29 after sell → book $9,101.28; vs 09:30 mark -0.09 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `EXK` | 1 | $10.82 | $0.13 | $-0.19 | $42.38 | ▼ -0.19 after sell → book $9,101.15; vs 09:30 mark -0.13 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `SCZM` | 1 | $9.61 | $0.12 | $-0.07 | $51.87 | ▼ -0.07 after sell → book $9,101.03; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-27 09:30 ET | **SELL** | `NG` | 1 | $9.55 | $0.12 | $+0.96 | $61.30 | ▲ +0.96 after sell → book $9,100.91; vs 09:30 mark -0.12 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $61.30 | ▲ 09:30 equity $9,020.83 vs yday $8,951.10 (+69.73) | 09:30 open · cash $61.30 (unchanged overnight, no fees) · equity $9,020.83 vs prior close $8,951.10 (+69.73) because holdings re-marked: BTBT×776 yday $1.56 → 09:30 $1.59 +23.28; ENHA×753 yday $1.61 → 09:30 $1.64 +22.59; DE×2 yday $634.54 → 09:30 $628.82 -11.44; QDEL×86 yday $14.91 → 09:30 $14.92 +0.86; ORBS×1491 yday $0.80 → 09:30 $0.82 +29.82; GORO×414 yday $3.56 → 09:30 $3.59 +12.42; QTRX×413 yday $2.68 → 09:30 $2.66 -8.26; BMEA×12 yday $1.71 → 09:30 $1.74 +0.36; NPWR×9 yday $1.81 → 09:30 $1.83 +0.18; PUSA×5 yday $3.85 → 09:30 $3.86 +0.05; ALVO×3 yday $4.91 → 09:30 $4.88 -0.09; CAPR×2 yday $9.36 → 09:30 $9.19 -0.34; ALIT×1 yday $14.33 → 09:30 $14.54 +0.21; ZURA×3 yday $5.99 → 09:30 $6.02 +0.09; SAFX×52 yday $0.39 → 09:30 $0.39 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `BTBT` | 776 | $1.59 | $10.15 | $-74.48 | $1,284.99 | ▼ -74.48 after sell → book $9,010.68; vs 09:30 mark -10.15 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ENHA` | 753 | $1.64 | $9.85 | $-72.27 | $2,510.06 | ▼ -72.27 after sell → book $9,000.83; vs 09:30 mark -9.85 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `DE` | 2 | $628.82 | $2.02 | $+7.11 | $3,765.68 | ▲ +7.11 after sell → book $8,998.81; vs 09:30 mark -2.02 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QDEL` | 86 | $14.92 | $2.27 | $-7.96 | $5,046.53 | ▼ -7.96 after sell → book $8,996.54; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1491 | $0.82 | $16.96 | $-99.92 | $6,252.20 | ▼ -99.92 after sell → book $8,979.59; vs 09:30 mark -16.95 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `GORO` | 414 | $3.59 | $5.42 | $+187.96 | $7,733.03 | ▲ +187.96 after sell → book $8,974.16; vs 09:30 mark -5.43 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QTRX` | 413 | $2.66 | $5.41 | $-196.58 | $8,826.21 | ▼ -196.58 after sell → book $8,968.76; vs 09:30 mark -5.40 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $7,666.59 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1260.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 74 | $16.95 | $2.21 | — | $6,410.08 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1260.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 68 | $18.50 | $2.19 | — | $5,149.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1260.89 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 913 | $1.38 | $11.78 | — | $3,878.17 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1260.89 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 37 | $33.78 | $2.10 | — | $2,626.21 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1260.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $1,429.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1260.89 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 79 | $15.94 | $2.23 | — | $167.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1260.89 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.51 | ▼ 09:30 equity $8,467.86 vs yday $8,750.40 (-282.54) | 09:30 open · cash $167.51 (unchanged overnight, no fees) · equity $8,467.86 vs prior close $8,750.40 (-282.54) because holdings re-marked: BMEA×12 yday $1.68 → 09:30 $1.71 +0.36; NPWR×9 yday $1.89 → 09:30 $1.83 -0.54; PUSA×5 yday $3.79 → 09:30 $3.72 -0.35; ALVO×3 yday $4.88 → 09:30 $4.98 +0.30; CAPR×2 yday $10.06 → 09:30 $9.44 -1.24; ALIT×1 yday $14.21 → 09:30 $14.30 +0.09; ZURA×3 yday $5.85 → 09:30 $5.51 -1.02; SAFX×52 yday $0.37 → 09:30 $0.38 +0.52; ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×74 yday $16.12 → 09:30 $15.44 -50.32; BZ×68 yday $18.00 → 09:30 $17.89 -7.48; LVWR×913 yday $1.36 → 09:30 $1.37 +9.13; SEDG×37 yday $33.51 → 09:30 $31.50 -74.37; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×79 yday $15.66 → 09:30 $14.32 -105.86 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $167.51 | ▼ 09:30 equity $8,322.69 vs yday $8,420.76 (-98.07) | 09:30 open · cash $167.51 (unchanged overnight, no fees) · equity $8,322.69 vs prior close $8,420.76 (-98.07) because holdings re-marked: BMEA×12 yday $1.71 → 09:30 $1.65 -0.72; NPWR×9 yday $1.82 → 09:30 $1.78 -0.36; PUSA×5 yday $3.80 → 09:30 $3.93 +0.65; ALVO×3 yday $4.96 → 09:30 $5.24 +0.84; CAPR×2 yday $9.36 → 09:30 $10.43 +2.14; ALIT×1 yday $14.02 → 09:30 $14.72 +0.70; ZURA×3 yday $5.64 → 09:30 $5.60 -0.12; SAFX×52 yday $0.37 → 09:30 $0.37 +0.00; ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BHVN×74 yday $15.40 → 09:30 $15.45 +3.70; BZ×68 yday $17.90 → 09:30 $17.37 -36.04; LVWR×913 yday $1.34 → 09:30 $1.22 -109.56; SEDG×37 yday $31.27 → 09:30 $32.22 +35.15; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×79 yday $14.20 → 09:30 $15.05 +67.15 | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 12 | $1.65 | $0.25 | $-0.12 | $187.05 | ▼ -0.12 after sell → book $8,322.43; vs 09:30 mark -0.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 9 | $1.78 | $0.21 | $-2.39 | $202.87 | ▼ -2.39 after sell → book $8,322.23; vs 09:30 mark -0.20 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `PUSA` | 5 | $3.93 | $0.23 | $+0.72 | $222.29 | ▲ +0.72 after sell → book $8,322.00; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 3 | $5.24 | $0.19 | $-0.29 | $237.82 | ▼ -0.29 after sell → book $8,321.81; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 2 | $10.43 | $0.23 | $+6.90 | $258.44 | ▲ +6.90 after sell → book $8,321.57; vs 09:30 mark -0.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALIT` | 1 | $14.72 | $0.17 | $-0.46 | $272.99 | ▼ -0.46 after sell → book $8,321.40; vs 09:30 mark -0.17 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 3 | $5.60 | $0.20 | $-2.74 | $289.60 | ▼ -2.74 after sell → book $8,321.21; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 52 | $0.37 | $0.37 | $-0.72 | $308.47 | ▼ -0.72 after sell → book $8,320.84; vs 09:30 mark -0.37 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.47 | ▼ 09:30 equity $8,211.00 vs yday $8,222.47 (-11.47) | 09:30 open · cash $308.47 (unchanged overnight, no fees) · equity $8,211.00 vs prior close $8,222.47 (-11.47) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BHVN×74 yday $15.45 → 09:30 $15.39 -4.44; BZ×68 yday $17.17 → 09:30 $17.29 +8.16; LVWR×913 yday $1.18 → 09:30 $1.19 +9.13; SEDG×37 yday $31.80 → 09:30 $31.87 +2.59; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×79 yday $14.80 → 09:30 $14.75 -3.95 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $308.47 | ▲ 09:30 equity $8,239.08 vs yday $8,206.29 (+32.79) | 09:30 open · cash $308.47 (unchanged overnight, no fees) · equity $8,239.08 vs prior close $8,206.29 (+32.79) because holdings re-marked: ANF×8 yday $140.68 → 09:30 $139.65 -8.24; BHVN×74 yday $15.74 → 09:30 $15.97 +17.02; BZ×68 yday $17.55 → 09:30 $17.65 +6.80; LVWR×913 yday $1.14 → 09:30 $1.17 +27.39; SEDG×37 yday $32.49 → 09:30 $32.42 -2.59; SMTC×8 yday $132.27 → 09:30 $133.00 +5.84; GRRR×79 yday $14.09 → 09:30 $13.92 -13.43 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 31 | $1.22 | $0.47 | — | $270.18 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $38.56 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 2 | $18.40 | $0.37 | — | $233.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $38.56 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 2 | $15.70 | $0.32 | — | $201.28 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $38.56 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 1 | $22.78 | $0.23 | — | $178.27 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $38.56 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 10 | $3.72 | $0.40 | — | $140.67 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $38.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 2 | $14.70 | $0.30 | — | $110.97 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $38.56 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 17 | $2.16 | $0.42 | — | $73.83 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $38.56 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $73.83 | ▼ 09:30 equity $8,247.09 vs yday $8,258.49 (-11.40) | 09:30 open · cash $73.83 (unchanged overnight, no fees) · equity $8,247.09 vs prior close $8,258.49 (-11.40) because holdings re-marked: ANF×8 yday $136.60 → 09:30 $137.70 +8.80; BHVN×74 yday $15.69 → 09:30 $15.89 +14.80; BZ×68 yday $17.30 → 09:30 $17.31 +0.68; LVWR×913 yday $1.20 → 09:30 $1.17 -27.39; SEDG×37 yday $33.98 → 09:30 $33.69 -10.73; SMTC×8 yday $133.85 → 09:30 $133.10 -6.00; GRRR×79 yday $13.72 → 09:30 $13.78 +4.74; GPRO×31 yday $1.69 → 09:30 $1.78 +2.79; FRVO×2 yday $17.98 → 09:30 $18.27 +0.58; CRK×2 yday $15.54 → 09:30 $15.45 -0.18; MMED×1 yday $23.76 → 09:30 $23.88 +0.12; CTMX×10 yday $3.72 → 09:30 $3.73 +0.10; SLN×2 yday $14.79 → 09:30 $14.85 +0.12; CRDL×17 yday $2.17 → 09:30 $2.18 +0.17 | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 8 | $137.70 | $2.03 | $-60.05 | $1,173.40 | ▼ -60.05 after sell → book $8,245.06; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 74 | $15.89 | $2.23 | $-82.89 | $2,347.02 | ▼ -82.89 after sell → book $8,242.82; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 68 | $17.31 | $2.22 | $-85.33 | $3,521.89 | ▼ -85.33 after sell → book $8,240.61; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LVWR` | 913 | $1.17 | $11.94 | $-215.45 | $4,578.16 | ▼ -215.45 after sell → book $8,228.67; vs 09:30 mark -11.94 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 37 | $33.69 | $2.12 | $-7.55 | $5,822.57 | ▼ -7.55 after sell → book $8,226.55; vs 09:30 mark -2.12 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 8 | $133.10 | $2.03 | $-134.45 | $6,885.34 | ▼ -134.45 after sell → book $8,224.52; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 79 | $13.78 | $2.25 | $-175.12 | $7,971.71 | ▼ -175.12 after sell → book $8,222.27; vs 09:30 mark -2.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 584 | $1.95 | $7.53 | — | $6,825.37 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1138.82 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 318 | $3.57 | $4.10 | — | $5,686.01 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1138.82 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 370 | $3.07 | $4.77 | — | $4,545.34 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1138.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $3,570.72 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1138.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 39 | $29.15 | $2.11 | — | $2,431.76 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1138.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 111 | $10.22 | $2.32 | — | $1,295.02 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1138.82 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 155 | $7.31 | $2.46 | — | $159.52 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1138.82 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `WDC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `FOSL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AIRS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ALGM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 33.64 < 1 share @ 39.85 |
| 2026-08-17 | `CELC` | cash | leftover split 33.64 < 1 share @ 92.99 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `WDC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `FOSL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AIRS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ALGM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ABX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `FCEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `VERA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `BW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `OCC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `ALM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `WWW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `WDC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `FOSL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AIRS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ALGM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ABX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `FCEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `VERA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `BW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `OCC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `ALM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNTN` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `MNDY` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `WWW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `WDC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `FOSL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AIRS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ALGM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ABX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `FCEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `VERA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `BW` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `OCC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `ALM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-20 | `MSTR` | cash | leftover split 13.09 < 1 share @ 113.23 |
| 2026-08-20 | `BLSH` | cash | leftover split 13.09 < 1 share @ 29.20 |
| 2026-08-20 | `HYMC` | cash | leftover split 13.09 < 1 share @ 27.25 |
| 2026-08-21 | `ABX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `FCEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `VERA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `BW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `OCC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `ALM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `DE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QTRX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `DNA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `EXK` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `SCZM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `NG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `DE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `GORO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QTRX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `MRVI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `DNA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `EXK` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `SCZM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `NG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-26 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ENHA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `DE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QDEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `GORO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QTRX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-26 | `BE` | no_price | no 09:30 open |
| 2026-08-27 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ENHA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QDEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `GORO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QTRX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALIT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SAFX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `PUSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALIT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SAFX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `PUSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALIT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SAFX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `LVWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `GRRR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `LVWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `GRRR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `TRGP` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NEOV` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `AME` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NMRA` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `CTMX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `SLDB` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NTNX` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-01 | `NAGE` | hard_red | hard-red S=-6.30 sit; no new buys |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `LVWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GRRR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FATE` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `TII` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `AVXL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `BMO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `KMX` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `ZNTL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `LVWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `GRRR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `EIX` | cash | leftover split 38.56 < 1 share @ 56.78 |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 31 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $38.56 |
| `FRVO` | 2 | 2026-09-03 @ $18.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $38.56 |
| `CRK` | 2 | 2026-09-03 @ $15.70 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $38.56 |
| `MMED` | 1 | 2026-09-03 @ $22.78 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $38.56 |
| `CTMX` | 10 | 2026-09-03 @ $3.72 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $38.56 |
| `SLN` | 2 | 2026-09-03 @ $14.70 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $38.56 |
| `CRDL` | 17 | 2026-09-03 @ $2.16 | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $38.56 |
| `BAK` | 584 | 2026-09-04 @ $1.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1138.82 |
| `EOSE` | 318 | 2026-09-04 @ $3.57 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1138.82 |
| `SLBT` | 370 | 2026-09-04 @ $3.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1138.82 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1138.82 |
| `MLYS` | 39 | 2026-09-04 @ $29.15 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1138.82 |
| `CCOI` | 111 | 2026-09-04 @ $10.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1138.82 |
| `SION` | 155 | 2026-09-04 @ $7.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1138.82 |
