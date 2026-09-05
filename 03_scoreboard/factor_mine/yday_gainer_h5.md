# Factor mine action — `yday_gainer_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-8.09%** ($9,191) · signal-only (no cash/fees) was +8.88%. Starts YES **7/17**. Fills 77 · skips 200 · realized $-681.13.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $286.00.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | — | $4.90 | $9,804.72 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,850.47 | +45.75 | — | — | $4.90 | $9,771.77 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,850.47 vs prior close $9,804.72 (+45.75) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; OMER×72 yday $17.19 → 09:30 $17.17 -1.44; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; MXCT×899 yday $1.32 → 09:30 $1.32 +0.00 |
| 2026-08-18 | -6.20 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,666.38 | -105.39 | — | — | $4.90 | $9,551.67 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,666.38 vs prior close $9,771.77 (-105.39) because holdings re-marked: ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; WWW×60 yday $19.83 → 09:30 $19.95 +7.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; OMER×72 yday $17.36 → 09:30 $17.03 -23.76; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28; MXCT×899 yday $1.32 → 09:30 $1.30 -17.98 |
| 2026-08-19 | -7.20 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,589.58 | +37.91 | — | — | $4.90 | $9,679.61 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,589.58 vs prior close $9,551.67 (+37.91) because holdings re-marked: ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; WWW×60 yday $19.99 → 09:30 $20.08 +5.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; OMER×72 yday $17.19 → 09:30 $17.13 -4.32; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56; MXCT×899 yday $1.27 → 09:30 $1.29 +17.98 |
| 2026-08-20 | +1.12 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,572.33 | -107.28 | — | — | $4.90 | $9,427.80 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,572.33 vs prior close $9,679.61 (-107.28) because holdings re-marked: ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; WWW×60 yday $20.85 → 09:30 $20.15 -42.00; HYLN×299 yday $3.67 → 09:30 $3.61 -17.94; ARX×63 yday $19.55 → 09:30 $19.55 +0.00; OMER×72 yday $18.39 → 09:30 $18.30 -6.48; AIRO×112 yday $8.59 → 09:30 $8.51 -8.96; NCMI×464 yday $2.64 → 09:30 $2.59 -23.20; MXCT×899 yday $1.39 → 09:30 $1.39 +0.00 |
| 2026-08-21 | +3.25 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,487.85 | +60.05 | ARCT, CYPH, BTBT, MRVI, ENHA, DE, QDEL, ORBS | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | $523.76 | $9,749.66 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367 | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,487.85 vs prior close $9,427.80 (+60.05) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.43 +17.40; WWW×60 yday $20.45 → 09:30 $20.32 -7.80; HYLN×299 yday $3.37 → 09:30 $3.42 +14.95; ARX×63 yday $19.57 → 09:30 $19.57 +0.00; OMER×72 yday $18.63 → 09:30 $18.64 +0.72; AIRO×112 yday $8.24 → 09:30 $8.39 +16.80; NCMI×464 yday $2.55 → 09:30 $2.55 +0.00; MXCT×899 yday $1.38 → 09:30 $1.40 +17.98 |
| 2026-08-24 | -5.17 | $523.76 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367 | $10,126.15 | +376.49 | — | — | $523.76 | $9,852.92 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367 | 09:30 open · cash $523.76 (unchanged overnight, no fees) · equity $10,126.15 vs prior close $9,749.66 (+376.49) because holdings re-marked: ARCT×106 yday $13.45 → 09:30 $13.26 -20.14; CYPH×895 yday $1.42 → 09:30 $1.83 +366.95; BTBT×711 yday $1.53 → 09:30 $1.55 +14.22; MRVI×144 yday $8.70 → 09:30 $8.59 -15.84; ENHA×691 yday $1.72 → 09:30 $1.74 +13.82; DE×1 yday $647.47 → 09:30 $653.62 +6.15; QDEL×78 yday $14.74 → 09:30 $14.71 -2.34; ORBS×1367 yday $0.88 → 09:30 $0.89 +13.67 |
| 2026-08-25 | +1.80 | $523.76 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367 | $9,944.95 | +92.03 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | — | $14.01 | $9,885.95 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367, BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176 | 09:30 open · cash $523.76 (unchanged overnight, no fees) · equity $9,944.95 vs prior close $9,852.92 (+92.03) because holdings re-marked: ARCT×106 yday $13.76 → 09:30 $14.34 +61.48; CYPH×895 yday $1.64 → 09:30 $1.70 +53.70; BTBT×711 yday $1.56 → 09:30 $1.55 -7.11; MRVI×144 yday $8.26 → 09:30 $8.31 +7.20; ENHA×691 yday $1.69 → 09:30 $1.65 -27.64; DE×1 yday $654.38 → 09:30 $648.64 -5.74; QDEL×78 yday $14.36 → 09:30 $14.49 +10.14; ORBS×1367 yday $0.85 → 09:30 $0.85 +0.00 |
| 2026-08-26 | +2.02 | $14.01 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367, BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176 | $9,885.95 | +0.00 | — | — | $14.01 | $9,939.01 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367, BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176 | 09:30 open · cash $14.01 (unchanged overnight, no fees) · equity $9,885.95 vs prior close $9,885.95 (+0.00) because holdings re-marked: ARCT×106 yday $14.21 → 09:30 $14.21 +0.00; CYPH×895 yday $1.64 → 09:30 $1.64 +0.00; BTBT×711 yday $1.53 → 09:30 $1.53 +0.00; MRVI×144 yday $8.49 → 09:30 $8.49 +0.00; ENHA×691 yday $1.66 → 09:30 $1.66 +0.00; DE×1 yday $649.11 → 09:30 $649.11 +0.00; QDEL×78 yday $14.49 → 09:30 $14.49 +0.00; ORBS×1367 yday $0.84 → 09:30 $0.84 +0.00; BMEA×40 yday $1.61 → 09:30 $1.61 +0.00; NPWR×32 yday $2.02 → 09:30 $2.02 +0.00; PUSA×17 yday $3.91 → 09:30 $3.91 +0.00; ALVO×12 yday $5.25 → 09:30 $5.25 +0.00; CAPR×9 yday $7.19 → 09:30 $7.19 +0.00; ALIT×4 yday $14.87 → 09:30 $14.87 +0.00; ZURA×10 yday $6.50 → 09:30 $6.50 +0.00; SAFX×176 yday $0.37 → 09:30 $0.37 +0.00 |
| 2026-08-27 | — | $14.01 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367, BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176 | $9,978.15 | +39.14 | — | — | $14.01 | $10,066.02 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367, BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176 | 09:30 open · cash $14.01 (unchanged overnight, no fees) · equity $9,978.15 vs prior close $9,939.01 (+39.14) because holdings re-marked: ARCT×106 yday $14.21 → 09:30 $15.35 +120.84; CYPH×895 yday $1.64 → 09:30 $1.60 -35.80; BTBT×711 yday $1.53 → 09:30 $1.53 +0.00; MRVI×144 yday $8.49 → 09:30 $8.85 +51.84; ENHA×691 yday $1.66 → 09:30 $1.63 -20.73; DE×1 yday $649.11 → 09:30 $632.15 -16.96; QDEL×78 yday $14.49 → 09:30 $15.09 +46.80; ORBS×1367 yday $0.84 → 09:30 $0.80 -54.68; BMEA×40 yday $1.61 → 09:30 $1.75 +5.60; NPWR×32 yday $2.02 → 09:30 $1.93 -2.88; PUSA×17 yday $3.91 → 09:30 $3.84 -1.19; ALVO×12 yday $5.25 → 09:30 $4.98 -3.24; CAPR×9 yday $7.19 → 09:30 $8.29 +9.90; ALIT×4 yday $14.87 → 09:30 $14.85 -0.08; ZURA×10 yday $6.50 → 09:30 $6.13 -3.70; SAFX×176 yday $0.37 → 09:30 $0.35 -3.52 |
| 2026-08-28 | +0.75 | $14.01 | ARCT×106, CYPH×895, BTBT×711, MRVI×144, ENHA×691, DE×1, QDEL×78, ORBS×1367, BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176 | $10,209.44 | +143.42 | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | ARCT, CYPH, BTBT, MRVI, ENHA, DE, QDEL, ORBS | $120.30 | $9,916.39 | BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176, ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86 | 09:30 open · cash $14.01 (unchanged overnight, no fees) · equity $10,209.44 vs prior close $10,066.02 (+143.42) because holdings re-marked: ARCT×106 yday $15.83 → 09:30 $15.74 -9.54; CYPH×895 yday $1.63 → 09:30 $1.75 +107.40; BTBT×711 yday $1.56 → 09:30 $1.59 +21.33; MRVI×144 yday $8.90 → 09:30 $8.76 -20.16; ENHA×691 yday $1.61 → 09:30 $1.64 +20.73; DE×1 yday $634.54 → 09:30 $628.82 -5.72; QDEL×78 yday $14.91 → 09:30 $14.92 +0.78; ORBS×1367 yday $0.80 → 09:30 $0.82 +27.34; BMEA×40 yday $1.71 → 09:30 $1.74 +1.20; NPWR×32 yday $1.81 → 09:30 $1.83 +0.64; PUSA×17 yday $3.85 → 09:30 $3.86 +0.17; ALVO×12 yday $4.91 → 09:30 $4.88 -0.36; CAPR×9 yday $9.36 → 09:30 $9.19 -1.53; ALIT×4 yday $14.33 → 09:30 $14.54 +0.84; ZURA×10 yday $5.99 → 09:30 $6.02 +0.30; SAFX×176 yday $0.39 → 09:30 $0.39 +0.00 |
| 2026-08-31 | -5.85 | $120.30 | BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176, ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86 | $9,601.70 | -314.69 | — | — | $120.30 | $9,549.26 | BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176, ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86 | 09:30 open · cash $120.30 (unchanged overnight, no fees) · equity $9,601.70 vs prior close $9,916.39 (-314.69) because holdings re-marked: BMEA×40 yday $1.68 → 09:30 $1.71 +1.20; NPWR×32 yday $1.89 → 09:30 $1.83 -1.92; PUSA×17 yday $3.79 → 09:30 $3.72 -1.19; ALVO×12 yday $4.88 → 09:30 $4.98 +1.20; CAPR×9 yday $10.06 → 09:30 $9.44 -5.58; ALIT×4 yday $14.21 → 09:30 $14.30 +0.36; ZURA×10 yday $5.85 → 09:30 $5.51 -3.40; SAFX×176 yday $0.37 → 09:30 $0.38 +1.76; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×81 yday $16.12 → 09:30 $15.44 -55.08; BZ×74 yday $18.00 → 09:30 $17.89 -8.14; LVWR×997 yday $1.36 → 09:30 $1.37 +9.97; SEDG×40 yday $33.51 → 09:30 $31.50 -80.40; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; GRRR×86 yday $15.66 → 09:30 $14.32 -115.24 |
| 2026-09-01 | -6.30 | $120.30 | BMEA×40, NPWR×32, PUSA×17, ALVO×12, CAPR×9, ALIT×4, ZURA×10, SAFX×176, ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86 | $9,450.17 | -99.09 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | $640.47 | $9,336.26 | ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86 | 09:30 open · cash $120.30 (unchanged overnight, no fees) · equity $9,450.17 vs prior close $9,549.26 (-99.09) because holdings re-marked: BMEA×40 yday $1.71 → 09:30 $1.65 -2.40; NPWR×32 yday $1.82 → 09:30 $1.78 -1.28; PUSA×17 yday $3.80 → 09:30 $3.93 +2.21; ALVO×12 yday $4.96 → 09:30 $5.24 +3.36; CAPR×9 yday $9.36 → 09:30 $10.43 +9.63; ALIT×4 yday $14.02 → 09:30 $14.72 +2.80; ZURA×10 yday $5.64 → 09:30 $5.60 -0.40; SAFX×176 yday $0.37 → 09:30 $0.37 +0.00; ANF×9 yday $149.28 → 09:30 $142.47 -61.29; BHVN×81 yday $15.40 → 09:30 $15.45 +4.05; BZ×74 yday $17.90 → 09:30 $17.37 -39.22; LVWR×997 yday $1.34 → 09:30 $1.22 -119.64; SEDG×40 yday $31.27 → 09:30 $32.22 +38.00; SMTC×9 yday $132.54 → 09:30 $131.65 -8.01; GRRR×86 yday $14.20 → 09:30 $15.05 +73.10 |
| 2026-09-02 | -3.83 | $640.47 | ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86 | $9,322.92 | -13.34 | — | — | $640.47 | $9,318.58 | ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86 | 09:30 open · cash $640.47 (unchanged overnight, no fees) · equity $9,322.92 vs prior close $9,336.26 (-13.34) because holdings re-marked: ANF×9 yday $143.00 → 09:30 $142.00 -9.00; BHVN×81 yday $15.45 → 09:30 $15.39 -4.86; BZ×74 yday $17.17 → 09:30 $17.29 +8.88; LVWR×997 yday $1.18 → 09:30 $1.19 +9.97; SEDG×40 yday $31.80 → 09:30 $31.87 +2.80; SMTC×9 yday $129.50 → 09:30 $127.63 -16.83; GRRR×86 yday $14.80 → 09:30 $14.75 -4.30 |
| 2026-09-03 | -0.90 | $640.47 | ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86 | $9,354.40 | +35.82 | GPRO, FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | — | $46.11 | $9,385.07 | ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86, GPRO×65, FRVO×4, CRK×5, MMED×3, CTMX×21, SLN×5, EIX×1, CRDL×37 | 09:30 open · cash $640.47 (unchanged overnight, no fees) · equity $9,354.40 vs prior close $9,318.58 (+35.82) because holdings re-marked: ANF×9 yday $140.68 → 09:30 $139.65 -9.27; BHVN×81 yday $15.74 → 09:30 $15.97 +18.63; BZ×74 yday $17.55 → 09:30 $17.65 +7.40; LVWR×997 yday $1.14 → 09:30 $1.17 +29.91; SEDG×40 yday $32.49 → 09:30 $32.42 -2.80; SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; GRRR×86 yday $14.09 → 09:30 $13.92 -14.62 |
| 2026-09-04 | — | $46.11 | ANF×9, BHVN×81, BZ×74, LVWR×997, SEDG×40, SMTC×9, GRRR×86, GPRO×65, FRVO×4, CRK×5, MMED×3, CTMX×21, SLN×5, EIX×1, CRDL×37 | $9,376.84 | -8.23 | BAK, EOSE, SLBT, DELL, MLYS, CCOI, SION | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | $286.00 | $9,190.82 | GPRO×65, FRVO×4, CRK×5, MMED×3, CTMX×21, SLN×5, EIX×1, CRDL×37, BAK×639, EOSE×349, SLBT×405, DELL×2, MLYS×42, CCOI×121, SION×170 | 09:30 open · cash $46.11 (unchanged overnight, no fees) · equity $9,376.84 vs prior close $9,385.07 (-8.23) because holdings re-marked: ANF×9 yday $136.60 → 09:30 $137.70 +9.90; BHVN×81 yday $15.69 → 09:30 $15.89 +16.20; BZ×74 yday $17.30 → 09:30 $17.31 +0.74; LVWR×997 yday $1.20 → 09:30 $1.17 -29.91; SEDG×40 yday $33.98 → 09:30 $33.69 -11.60; SMTC×9 yday $133.85 → 09:30 $133.10 -6.75; GRRR×86 yday $13.72 → 09:30 $13.78 +5.16; GPRO×65 yday $1.69 → 09:30 $1.78 +5.85; FRVO×4 yday $17.98 → 09:30 $18.27 +1.16; CRK×5 yday $15.54 → 09:30 $15.45 -0.45; MMED×3 yday $23.76 → 09:30 $23.88 +0.36; CTMX×21 yday $3.72 → 09:30 $3.73 +0.21; SLN×5 yday $14.79 → 09:30 $14.85 +0.30; EIX×1 yday $55.19 → 09:30 $55.42 +0.23; CRDL×37 yday $2.17 → 09:30 $2.18 +0.37 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $8,746.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `WWW` | 60 | $20.60 | $2.17 | — | $7,508.19 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+4.4; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟢 vol🟡 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $6,254.51 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $5,019.42 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `OMER` | 72 | $17.35 | $2.21 | — | $3,768.02 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+31.9; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,520.25 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,266.11 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `MXCT` | 899 | $1.39 | $11.60 | — | $4.90 | — | baseline list, no extra gate; list yday_gainer,yday_mover; 🔵; ret5=+25.2; leftover $1250.00 | join🟢 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▲ 09:30 equity $9,850.47 vs yday $9,804.72 (+45.75) | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,850.47 vs prior close $9,804.72 (+45.75) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; OMER×72 yday $17.19 → 09:30 $17.17 -1.44; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; MXCT×899 yday $1.32 → 09:30 $1.32 +0.00 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▼ 09:30 equity $9,666.38 vs yday $9,771.77 (-105.39) | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,666.38 vs prior close $9,771.77 (-105.39) because holdings re-marked: ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; WWW×60 yday $19.83 → 09:30 $19.95 +7.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; OMER×72 yday $17.36 → 09:30 $17.03 -23.76; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28; MXCT×899 yday $1.32 → 09:30 $1.30 -17.98 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▲ 09:30 equity $9,589.58 vs yday $9,551.67 (+37.91) | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,589.58 vs prior close $9,551.67 (+37.91) because holdings re-marked: ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; WWW×60 yday $19.99 → 09:30 $20.08 +5.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; OMER×72 yday $17.19 → 09:30 $17.13 -4.32; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56; MXCT×899 yday $1.27 → 09:30 $1.29 +17.98 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▼ 09:30 equity $9,572.33 vs yday $9,679.61 (-107.28) | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,572.33 vs prior close $9,679.61 (-107.28) because holdings re-marked: ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; WWW×60 yday $20.85 → 09:30 $20.15 -42.00; HYLN×299 yday $3.67 → 09:30 $3.61 -17.94; ARX×63 yday $19.55 → 09:30 $19.55 +0.00; OMER×72 yday $18.39 → 09:30 $18.30 -6.48; AIRO×112 yday $8.59 → 09:30 $8.51 -8.96; NCMI×464 yday $2.64 → 09:30 $2.59 -23.20; MXCT×899 yday $1.39 → 09:30 $1.39 +0.00 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $4.90 | ▲ 09:30 equity $9,487.85 vs yday $9,427.80 (+60.05) | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,487.85 vs prior close $9,427.80 (+60.05) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.43 +17.40; WWW×60 yday $20.45 → 09:30 $20.32 -7.80; HYLN×299 yday $3.37 → 09:30 $3.42 +14.95; ARX×63 yday $19.57 → 09:30 $19.57 +0.00; OMER×72 yday $18.63 → 09:30 $18.64 +0.72; AIRO×112 yday $8.24 → 09:30 $8.39 +16.80; NCMI×464 yday $2.55 → 09:30 $2.55 +0.00; MXCT×899 yday $1.38 → 09:30 $1.40 +17.98 | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $1,285.80 | ▲ +27.26 after sell → book $9,484.05; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `WWW` | 60 | $20.32 | $2.19 | $-21.16 | $2,502.81 | ▼ -21.16 after sell → book $9,481.86; vs 09:30 mark -2.19 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 299 | $3.42 | $3.92 | $-235.01 | $3,521.47 | ▼ -235.01 after sell → book $9,477.94; vs 09:30 mark -3.92 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $4,752.18 | ▼ -4.38 after sell → book $9,475.74; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `OMER` | 72 | $18.64 | $2.23 | $+88.45 | $6,092.03 | ▲ +88.45 after sell → book $9,473.51; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AIRO` | 112 | $8.39 | $2.35 | $-310.44 | $7,029.36 | ▼ -310.44 after sell → book $9,471.16; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NCMI` | 464 | $2.55 | $6.07 | $-77.02 | $8,206.49 | ▼ -77.02 after sell → book $9,465.09; vs 09:30 mark -6.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `MXCT` | 899 | $1.40 | $11.76 | $-14.36 | $9,453.33 | ▼ -14.36 after sell → book $9,453.33; vs 09:30 mark -11.76 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 106 | $11.13 | $2.31 | — | $8,271.24 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1181.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 895 | $1.32 | $11.55 | — | $7,078.30 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1181.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 711 | $1.66 | $9.17 | — | $5,888.86 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1181.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `MRVI` | 144 | $8.20 | $2.42 | — | $4,705.64 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1181.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 691 | $1.71 | $8.91 | — | $3,515.12 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1181.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $2,889.87 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1181.67 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 78 | $14.96 | $2.22 | — | $1,720.76 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1181.67 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1367 | $0.86 | $15.91 | — | $523.76 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1181.67 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $523.76 | ▲ 09:30 equity $10,126.15 vs yday $9,749.66 (+376.49) | 09:30 open · cash $523.76 (unchanged overnight, no fees) · equity $10,126.15 vs prior close $9,749.66 (+376.49) because holdings re-marked: ARCT×106 yday $13.45 → 09:30 $13.26 -20.14; CYPH×895 yday $1.42 → 09:30 $1.83 +366.95; BTBT×711 yday $1.53 → 09:30 $1.55 +14.22; MRVI×144 yday $8.70 → 09:30 $8.59 -15.84; ENHA×691 yday $1.72 → 09:30 $1.74 +13.82; DE×1 yday $647.47 → 09:30 $653.62 +6.15; QDEL×78 yday $14.74 → 09:30 $14.71 -2.34; ORBS×1367 yday $0.88 → 09:30 $0.89 +13.67 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $523.76 | ▲ 09:30 equity $9,944.95 vs yday $9,852.92 (+92.03) | 09:30 open · cash $523.76 (unchanged overnight, no fees) · equity $9,944.95 vs prior close $9,852.92 (+92.03) because holdings re-marked: ARCT×106 yday $13.76 → 09:30 $14.34 +61.48; CYPH×895 yday $1.64 → 09:30 $1.70 +53.70; BTBT×711 yday $1.56 → 09:30 $1.55 -7.11; MRVI×144 yday $8.26 → 09:30 $8.31 +7.20; ENHA×691 yday $1.69 → 09:30 $1.65 -27.64; DE×1 yday $654.38 → 09:30 $648.64 -5.74; QDEL×78 yday $14.36 → 09:30 $14.49 +10.14; ORBS×1367 yday $0.85 → 09:30 $0.85 +0.00 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 40 | $1.62 | $0.77 | — | $458.19 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $65.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 32 | $2.00 | $0.74 | — | $393.46 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $65.47 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 17 | $3.70 | $0.68 | — | $329.88 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $65.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 12 | $5.22 | $0.66 | — | $266.58 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $65.47 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 9 | $6.79 | $0.64 | — | $204.83 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $65.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 4 | $14.86 | $0.61 | — | $144.78 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $65.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 10 | $6.38 | $0.67 | — | $80.31 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $65.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 176 | $0.37 | $1.18 | — | $14.01 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $65.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.01 | ▲ 09:30 equity $9,885.95 vs yday $9,885.95 (+0.00) | 09:30 open · cash $14.01 (unchanged overnight, no fees) · equity $9,885.95 vs prior close $9,885.95 (+0.00) because holdings re-marked: ARCT×106 yday $14.21 → 09:30 $14.21 +0.00; CYPH×895 yday $1.64 → 09:30 $1.64 +0.00; BTBT×711 yday $1.53 → 09:30 $1.53 +0.00; MRVI×144 yday $8.49 → 09:30 $8.49 +0.00; ENHA×691 yday $1.66 → 09:30 $1.66 +0.00; DE×1 yday $649.11 → 09:30 $649.11 +0.00; QDEL×78 yday $14.49 → 09:30 $14.49 +0.00; ORBS×1367 yday $0.84 → 09:30 $0.84 +0.00; BMEA×40 yday $1.61 → 09:30 $1.61 +0.00; NPWR×32 yday $2.02 → 09:30 $2.02 +0.00; PUSA×17 yday $3.91 → 09:30 $3.91 +0.00; ALVO×12 yday $5.25 → 09:30 $5.25 +0.00; CAPR×9 yday $7.19 → 09:30 $7.19 +0.00; ALIT×4 yday $14.87 → 09:30 $14.87 +0.00; ZURA×10 yday $6.50 → 09:30 $6.50 +0.00; SAFX×176 yday $0.37 → 09:30 $0.37 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.01 | ▲ 09:30 equity $9,978.15 vs yday $9,939.01 (+39.14) | 09:30 open · cash $14.01 (unchanged overnight, no fees) · equity $9,978.15 vs prior close $9,939.01 (+39.14) because holdings re-marked: ARCT×106 yday $14.21 → 09:30 $15.35 +120.84; CYPH×895 yday $1.64 → 09:30 $1.60 -35.80; BTBT×711 yday $1.53 → 09:30 $1.53 +0.00; MRVI×144 yday $8.49 → 09:30 $8.85 +51.84; ENHA×691 yday $1.66 → 09:30 $1.63 -20.73; DE×1 yday $649.11 → 09:30 $632.15 -16.96; QDEL×78 yday $14.49 → 09:30 $15.09 +46.80; ORBS×1367 yday $0.84 → 09:30 $0.80 -54.68; BMEA×40 yday $1.61 → 09:30 $1.75 +5.60; NPWR×32 yday $2.02 → 09:30 $1.93 -2.88; PUSA×17 yday $3.91 → 09:30 $3.84 -1.19; ALVO×12 yday $5.25 → 09:30 $4.98 -3.24; CAPR×9 yday $7.19 → 09:30 $8.29 +9.90; ALIT×4 yday $14.87 → 09:30 $14.85 -0.08; ZURA×10 yday $6.50 → 09:30 $6.13 -3.70; SAFX×176 yday $0.37 → 09:30 $0.35 -3.52 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $14.01 | ▲ 09:30 equity $10,209.44 vs yday $10,066.02 (+143.42) | 09:30 open · cash $14.01 (unchanged overnight, no fees) · equity $10,209.44 vs prior close $10,066.02 (+143.42) because holdings re-marked: ARCT×106 yday $15.83 → 09:30 $15.74 -9.54; CYPH×895 yday $1.63 → 09:30 $1.75 +107.40; BTBT×711 yday $1.56 → 09:30 $1.59 +21.33; MRVI×144 yday $8.90 → 09:30 $8.76 -20.16; ENHA×691 yday $1.61 → 09:30 $1.64 +20.73; DE×1 yday $634.54 → 09:30 $628.82 -5.72; QDEL×78 yday $14.91 → 09:30 $14.92 +0.78; ORBS×1367 yday $0.80 → 09:30 $0.82 +27.34; BMEA×40 yday $1.71 → 09:30 $1.74 +1.20; NPWR×32 yday $1.81 → 09:30 $1.83 +0.64; PUSA×17 yday $3.85 → 09:30 $3.86 +0.17; ALVO×12 yday $4.91 → 09:30 $4.88 -0.36; CAPR×9 yday $9.36 → 09:30 $9.19 -1.53; ALIT×4 yday $14.33 → 09:30 $14.54 +0.84; ZURA×10 yday $5.99 → 09:30 $6.02 +0.30; SAFX×176 yday $0.39 → 09:30 $0.39 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 106 | $15.74 | $2.34 | $+484.01 | $1,680.12 | ▲ +484.01 after sell → book $10,207.11; vs 09:30 mark -2.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 895 | $1.75 | $11.71 | $+361.60 | $3,234.66 | ▲ +361.60 after sell → book $10,195.40; vs 09:30 mark -11.71 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `BTBT` | 711 | $1.59 | $9.30 | $-68.24 | $4,355.85 | ▼ -68.24 after sell → book $10,186.10; vs 09:30 mark -9.30 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `MRVI` | 144 | $8.76 | $2.46 | $+75.76 | $5,614.83 | ▲ +75.76 after sell → book $10,183.64; vs 09:30 mark -2.46 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ENHA` | 691 | $1.64 | $9.04 | $-66.32 | $6,739.03 | ▼ -66.32 after sell → book $10,174.60; vs 09:30 mark -9.04 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `DE` | 1 | $628.82 | $2.01 | $+1.55 | $7,365.84 | ▲ +1.55 after sell → book $10,172.59; vs 09:30 mark -2.01 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `QDEL` | 78 | $14.92 | $2.25 | $-7.59 | $8,527.35 | ▼ -7.59 after sell → book $10,170.34; vs 09:30 mark -2.25 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `ORBS` | 1367 | $0.82 | $15.55 | $-91.61 | $9,632.75 | ▼ -91.61 after sell → book $10,154.80; vs 09:30 mark -15.54 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $8,328.43 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1376.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 81 | $16.95 | $2.23 | — | $6,953.25 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1376.11 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 74 | $18.50 | $2.21 | — | $5,582.03 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1376.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 997 | $1.38 | $12.86 | — | $4,193.31 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1376.11 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $33.78 | $2.11 | — | $2,840.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1376.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $1,493.39 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1376.11 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 86 | $15.94 | $2.25 | — | $120.30 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1376.11 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.30 | ▼ 09:30 equity $9,601.70 vs yday $9,916.39 (-314.69) | 09:30 open · cash $120.30 (unchanged overnight, no fees) · equity $9,601.70 vs prior close $9,916.39 (-314.69) because holdings re-marked: BMEA×40 yday $1.68 → 09:30 $1.71 +1.20; NPWR×32 yday $1.89 → 09:30 $1.83 -1.92; PUSA×17 yday $3.79 → 09:30 $3.72 -1.19; ALVO×12 yday $4.88 → 09:30 $4.98 +1.20; CAPR×9 yday $10.06 → 09:30 $9.44 -5.58; ALIT×4 yday $14.21 → 09:30 $14.30 +0.36; ZURA×10 yday $5.85 → 09:30 $5.51 -3.40; SAFX×176 yday $0.37 → 09:30 $0.38 +1.76; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×81 yday $16.12 → 09:30 $15.44 -55.08; BZ×74 yday $18.00 → 09:30 $17.89 -8.14; LVWR×997 yday $1.36 → 09:30 $1.37 +9.97; SEDG×40 yday $33.51 → 09:30 $31.50 -80.40; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; GRRR×86 yday $15.66 → 09:30 $14.32 -115.24 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.30 | ▼ 09:30 equity $9,450.17 vs yday $9,549.26 (-99.09) | 09:30 open · cash $120.30 (unchanged overnight, no fees) · equity $9,450.17 vs prior close $9,549.26 (-99.09) because holdings re-marked: BMEA×40 yday $1.71 → 09:30 $1.65 -2.40; NPWR×32 yday $1.82 → 09:30 $1.78 -1.28; PUSA×17 yday $3.80 → 09:30 $3.93 +2.21; ALVO×12 yday $4.96 → 09:30 $5.24 +3.36; CAPR×9 yday $9.36 → 09:30 $10.43 +9.63; ALIT×4 yday $14.02 → 09:30 $14.72 +2.80; ZURA×10 yday $5.64 → 09:30 $5.60 -0.40; SAFX×176 yday $0.37 → 09:30 $0.37 +0.00; ANF×9 yday $149.28 → 09:30 $142.47 -61.29; BHVN×81 yday $15.40 → 09:30 $15.45 +4.05; BZ×74 yday $17.90 → 09:30 $17.37 -39.22; LVWR×997 yday $1.34 → 09:30 $1.22 -119.64; SEDG×40 yday $31.27 → 09:30 $32.22 +38.00; SMTC×9 yday $132.54 → 09:30 $131.65 -8.01; GRRR×86 yday $14.20 → 09:30 $15.05 +73.10 | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 40 | $1.65 | $0.80 | $-0.37 | $185.50 | ▼ -0.37 after sell → book $9,449.37; vs 09:30 mark -0.80 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 32 | $1.78 | $0.69 | $-8.46 | $241.77 | ▼ -8.46 after sell → book $9,448.68; vs 09:30 mark -0.69 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `PUSA` | 17 | $3.93 | $0.74 | $+2.49 | $307.84 | ▲ +2.49 after sell → book $9,447.94; vs 09:30 mark -0.74 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 12 | $5.24 | $0.68 | $-1.11 | $370.04 | ▼ -1.11 after sell → book $9,447.26; vs 09:30 mark -0.68 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 9 | $10.43 | $0.99 | $+31.14 | $462.92 | ▲ +31.14 after sell → book $9,446.27; vs 09:30 mark -0.99 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALIT` | 4 | $14.72 | $0.62 | $-1.79 | $521.18 | ▼ -1.79 after sell → book $9,445.65; vs 09:30 mark -0.62 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 10 | $5.60 | $0.61 | $-9.08 | $576.57 | ▼ -9.08 after sell → book $9,445.04; vs 09:30 mark -0.61 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SAFX` | 176 | $0.37 | $1.22 | $-2.40 | $640.47 | ▼ -2.40 after sell → book $9,443.82; vs 09:30 mark -1.22 | dropped from list after 5 sess (min 5) | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $640.47 | ▼ 09:30 equity $9,322.92 vs yday $9,336.26 (-13.34) | 09:30 open · cash $640.47 (unchanged overnight, no fees) · equity $9,322.92 vs prior close $9,336.26 (-13.34) because holdings re-marked: ANF×9 yday $143.00 → 09:30 $142.00 -9.00; BHVN×81 yday $15.45 → 09:30 $15.39 -4.86; BZ×74 yday $17.17 → 09:30 $17.29 +8.88; LVWR×997 yday $1.18 → 09:30 $1.19 +9.97; SEDG×40 yday $31.80 → 09:30 $31.87 +2.80; SMTC×9 yday $129.50 → 09:30 $127.63 -16.83; GRRR×86 yday $14.80 → 09:30 $14.75 -4.30 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $640.47 | ▲ 09:30 equity $9,354.40 vs yday $9,318.58 (+35.82) | 09:30 open · cash $640.47 (unchanged overnight, no fees) · equity $9,354.40 vs prior close $9,318.58 (+35.82) because holdings re-marked: ANF×9 yday $140.68 → 09:30 $139.65 -9.27; BHVN×81 yday $15.74 → 09:30 $15.97 +18.63; BZ×74 yday $17.55 → 09:30 $17.65 +7.40; LVWR×997 yday $1.14 → 09:30 $1.17 +29.91; SEDG×40 yday $32.49 → 09:30 $32.42 -2.80; SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; GRRR×86 yday $14.09 → 09:30 $13.92 -14.62 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 65 | $1.22 | $0.99 | — | $560.19 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $80.06 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 4 | $18.40 | $0.75 | — | $485.84 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $80.06 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 5 | $15.70 | $0.80 | — | $406.54 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $80.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 3 | $22.78 | $0.69 | — | $337.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $80.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 21 | $3.72 | $0.84 | — | $258.54 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $80.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 5 | $14.70 | $0.75 | — | $184.29 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $80.06 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 1 | $56.78 | $0.57 | — | $126.94 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $80.06 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 37 | $2.16 | $0.91 | — | $46.11 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $80.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $46.11 | ▼ 09:30 equity $9,376.84 vs yday $9,385.07 (-8.23) | 09:30 open · cash $46.11 (unchanged overnight, no fees) · equity $9,376.84 vs prior close $9,385.07 (-8.23) because holdings re-marked: ANF×9 yday $136.60 → 09:30 $137.70 +9.90; BHVN×81 yday $15.69 → 09:30 $15.89 +16.20; BZ×74 yday $17.30 → 09:30 $17.31 +0.74; LVWR×997 yday $1.20 → 09:30 $1.17 -29.91; SEDG×40 yday $33.98 → 09:30 $33.69 -11.60; SMTC×9 yday $133.85 → 09:30 $133.10 -6.75; GRRR×86 yday $13.72 → 09:30 $13.78 +5.16; GPRO×65 yday $1.69 → 09:30 $1.78 +5.85; FRVO×4 yday $17.98 → 09:30 $18.27 +1.16; CRK×5 yday $15.54 → 09:30 $15.45 -0.45; MMED×3 yday $23.76 → 09:30 $23.88 +0.36; CTMX×21 yday $3.72 → 09:30 $3.73 +0.21; SLN×5 yday $14.79 → 09:30 $14.85 +0.30; EIX×1 yday $55.19 → 09:30 $55.42 +0.23; CRDL×37 yday $2.17 → 09:30 $2.18 +0.37 | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 9 | $137.70 | $2.04 | $-67.05 | $1,283.37 | ▼ -67.05 after sell → book $9,374.80; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 81 | $15.89 | $2.26 | $-90.35 | $2,568.21 | ▼ -90.35 after sell → book $9,372.55; vs 09:30 mark -2.25 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 74 | $17.31 | $2.23 | $-92.51 | $3,846.91 | ▼ -92.51 after sell → book $9,370.31; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `LVWR` | 997 | $1.17 | $13.04 | $-235.27 | $5,000.37 | ▼ -235.27 after sell → book $9,357.28; vs 09:30 mark -13.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 40 | $33.69 | $2.13 | $-7.84 | $6,345.83 | ▼ -7.84 after sell → book $9,355.14; vs 09:30 mark -2.14 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 9 | $133.10 | $2.04 | $-150.75 | $7,541.70 | ▼ -150.75 after sell → book $9,353.11; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `GRRR` | 86 | $13.78 | $2.27 | $-190.28 | $8,724.51 | ▼ -190.28 after sell → book $9,350.84; vs 09:30 mark -2.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 639 | $1.95 | $8.24 | — | $7,470.21 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1246.36 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 349 | $3.57 | $4.50 | — | $6,219.78 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1246.36 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 405 | $3.07 | $5.22 | — | $4,971.21 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1246.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $3,996.59 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1246.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 42 | $29.15 | $2.12 | — | $2,770.17 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1246.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 121 | $10.22 | $2.35 | — | $1,531.20 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1246.36 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 170 | $7.31 | $2.50 | — | $286.00 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1246.36 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 0.61 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 0.61 < 1 share @ 9.12 |
| 2026-08-17 | `FCEL` | cash | leftover split 0.61 < 1 share @ 22.37 |
| 2026-08-17 | `VERA` | cash | leftover split 0.61 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 0.61 < 1 share @ 92.99 |
| 2026-08-17 | `CAPR` | cash | leftover split 0.61 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 0.61 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 0.61 < 1 share @ 32.55 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
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
| 2026-08-19 | `ARX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `OMER` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AIRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NCMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `MXCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `WWW` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ARX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `OMER` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AIRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NCMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `MXCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `CDE` | cash | leftover split 0.61 < 1 share @ 20.65 |
| 2026-08-20 | `MRVI` | cash | leftover split 0.61 < 1 share @ 7.38 |
| 2026-08-20 | `DNA` | cash | leftover split 0.61 < 1 share @ 7.45 |
| 2026-08-20 | `MSTR` | cash | leftover split 0.61 < 1 share @ 113.23 |
| 2026-08-20 | `EXK` | cash | leftover split 0.61 < 1 share @ 10.77 |
| 2026-08-20 | `SCZM` | cash | leftover split 0.61 < 1 share @ 9.46 |
| 2026-08-20 | `NG` | cash | leftover split 0.61 < 1 share @ 8.38 |
| 2026-08-20 | `BLSH` | cash | leftover split 0.61 < 1 share @ 29.20 |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `DE` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `MRVI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `DE` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `MRVI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ENHA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `DE` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `QDEL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ORBS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `RZLT` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `MRVI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ENHA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `DE` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `QDEL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ORBS` | min_hold | dropped but min-hold 4/5 sess — no sell |
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
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `SLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `EIX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 65 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $80.06 |
| `FRVO` | 4 | 2026-09-03 @ $18.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $80.06 |
| `CRK` | 5 | 2026-09-03 @ $15.70 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $80.06 |
| `MMED` | 3 | 2026-09-03 @ $22.78 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $80.06 |
| `CTMX` | 21 | 2026-09-03 @ $3.72 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $80.06 |
| `SLN` | 5 | 2026-09-03 @ $14.70 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $80.06 |
| `EIX` | 1 | 2026-09-03 @ $56.78 | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $80.06 |
| `CRDL` | 37 | 2026-09-03 @ $2.16 | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $80.06 |
| `BAK` | 639 | 2026-09-04 @ $1.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1246.36 |
| `EOSE` | 349 | 2026-09-04 @ $3.57 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1246.36 |
| `SLBT` | 405 | 2026-09-04 @ $3.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1246.36 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1246.36 |
| `MLYS` | 42 | 2026-09-04 @ $29.15 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1246.36 |
| `CCOI` | 121 | 2026-09-04 @ $10.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1246.36 |
| `SION` | 170 | 2026-09-04 @ $7.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1246.36 |
