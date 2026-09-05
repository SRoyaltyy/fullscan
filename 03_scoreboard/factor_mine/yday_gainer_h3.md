# Factor mine action — `yday_gainer_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **-5.11%** ($9,490) · signal-only (no cash/fees) was +13.89%. Starts YES **5/17**. Fills 86 · skips 152 · realized $-543.07.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $25.53.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | — | $4.90 | $9,804.72 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,850.47 | +45.75 | — | — | $4.90 | $9,771.77 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,850.47 vs prior close $9,804.72 (+45.75) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; OMER×72 yday $17.19 → 09:30 $17.17 -1.44; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; MXCT×899 yday $1.32 → 09:30 $1.32 +0.00 |
| 2026-08-18 | -6.20 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,666.38 | -105.39 | — | — | $4.90 | $9,551.67 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,666.38 vs prior close $9,771.77 (-105.39) because holdings re-marked: ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; WWW×60 yday $19.83 → 09:30 $19.95 +7.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; OMER×72 yday $17.36 → 09:30 $17.03 -23.76; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28; MXCT×899 yday $1.32 → 09:30 $1.30 -17.98 |
| 2026-08-19 | -7.20 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,589.58 | +37.91 | — | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | $9,555.06 | $9,555.06 | — | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,589.58 vs prior close $9,551.67 (+37.91) because holdings re-marked: ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; WWW×60 yday $19.99 → 09:30 $20.08 +5.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; OMER×72 yday $17.19 → 09:30 $17.13 -4.32; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56; MXCT×899 yday $1.27 → 09:30 $1.29 +17.98 |
| 2026-08-20 | +1.12 | $9,555.06 | — | $9,555.06 | +0.00 | CDE, MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH | — | $112.57 | $9,686.98 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40 | 09:30 open · cash $9,555.06 · no holdings · equity $9,555.06 vs prior close $9,555.06 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $112.57 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40 | $10,014.82 | +327.84 | ARCT, CYPH, BTBT, ENHA, QDEL, ORBS | — | $23.73 | $9,928.43 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40, ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18 | 09:30 open · cash $112.57 (unchanged overnight, no fees) · equity $10,014.82 vs prior close $9,686.98 (+327.84) because holdings re-marked: CDE×57 yday $21.11 → 09:30 $21.75 +36.48; MRVI×161 yday $8.26 → 09:30 $8.20 -9.66; DNA×160 yday $6.96 → 09:30 $7.09 +20.80; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×110 yday $10.97 → 09:30 $11.34 +40.70; SCZM×126 yday $9.76 → 09:30 $10.26 +63.00; NG×142 yday $8.66 → 09:30 $9.02 +51.12; BLSH×40 yday $28.44 → 09:30 $29.75 +52.40 |
| 2026-08-24 | -5.17 | $23.73 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40, ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18 | $10,011.30 | +82.87 | — | — | $23.73 | $9,906.03 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40, ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18 | 09:30 open · cash $23.73 (unchanged overnight, no fees) · equity $10,011.30 vs prior close $9,928.43 (+82.87) because holdings re-marked: CDE×57 yday $20.97 → 09:30 $21.26 +16.53; MRVI×161 yday $8.70 → 09:30 $8.59 -17.71; DNA×160 yday $7.40 → 09:30 $7.26 -22.40; MSTR×10 yday $119.25 → 09:30 $121.76 +25.10; EXK×110 yday $10.62 → 09:30 $11.01 +42.90; SCZM×126 yday $9.68 → 09:30 $9.82 +18.27; NG×142 yday $8.72 → 09:30 $8.89 +24.14; BLSH×40 yday $30.41 → 09:30 $30.18 -9.20; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; CYPH×12 yday $1.42 → 09:30 $1.83 +4.92; BTBT×9 yday $1.53 → 09:30 $1.55 +0.18; ENHA×9 yday $1.72 → 09:30 $1.74 +0.18; QDEL×1 yday $14.74 → 09:30 $14.71 -0.03; ORBS×18 yday $0.88 → 09:30 $0.89 +0.18 |
| 2026-08-25 | +1.80 | $23.73 | CDE×57, MRVI×161, DNA×160, MSTR×10, EXK×110, SCZM×126, NG×142, BLSH×40, ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18 | $9,941.52 | +35.49 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | CDE, MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH | $0.79 | $10,045.13 | ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18, BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | 09:30 open · cash $23.73 (unchanged overnight, no fees) · equity $9,941.52 vs prior close $9,906.03 (+35.49) because holdings re-marked: CDE×57 yday $20.49 → 09:30 $20.85 +20.52; MRVI×161 yday $8.26 → 09:30 $8.31 +8.05; DNA×160 yday $6.98 → 09:30 $6.82 -25.60; MSTR×10 yday $124.59 → 09:30 $125.56 +9.70; EXK×110 yday $10.74 → 09:30 $10.72 -2.20; SCZM×126 yday $9.53 → 09:30 $9.57 +5.04; NG×142 yday $9.24 → 09:30 $9.34 +14.20; BLSH×40 yday $30.88 → 09:30 $31.00 +4.80; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; CYPH×12 yday $1.64 → 09:30 $1.70 +0.72; BTBT×9 yday $1.56 → 09:30 $1.55 -0.09; ENHA×9 yday $1.69 → 09:30 $1.65 -0.36; QDEL×1 yday $14.36 → 09:30 $14.49 +0.13; ORBS×18 yday $0.85 → 09:30 $0.85 +0.00 |
| 2026-08-26 | +2.02 | $0.79 | ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18, BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | $10,045.13 | -0.00 | — | — | $0.79 | $9,868.92 | ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18, BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | 09:30 open · cash $0.79 (unchanged overnight, no fees) · equity $10,045.13 vs prior close $10,045.13 (-0.00) because holdings re-marked: ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; CYPH×12 yday $1.64 → 09:30 $1.64 +0.00; BTBT×9 yday $1.53 → 09:30 $1.53 +0.00; ENHA×9 yday $1.66 → 09:30 $1.66 +0.00; QDEL×1 yday $14.49 → 09:30 $14.49 +0.00; ORBS×18 yday $0.84 → 09:30 $0.84 +0.00; BMEA×758 yday $1.61 → 09:30 $1.61 +0.00; NPWR×614 yday $2.02 → 09:30 $2.02 +0.00; PUSA×332 yday $3.91 → 09:30 $3.91 +0.00; ALVO×235 yday $5.25 → 09:30 $5.25 +0.00; CAPR×180 yday $7.19 → 09:30 $7.19 +0.00; ALIT×82 yday $14.87 → 09:30 $14.87 +0.00; ZURA×192 yday $6.50 → 09:30 $6.50 +0.00; SAFX×3238 yday $0.37 → 09:30 $0.37 +0.00 |
| 2026-08-27 | — | $0.79 | ARCT×1, CYPH×12, BTBT×9, ENHA×9, QDEL×1, ORBS×18, BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | $10,070.13 | +201.21 | — | ARCT, CYPH, BTBT, ENHA, QDEL, ORBS | $92.07 | $10,204.40 | BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | 09:30 open · cash $0.79 (unchanged overnight, no fees) · equity $10,070.13 vs prior close $9,868.92 (+201.21) because holdings re-marked: ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; CYPH×12 yday $1.64 → 09:30 $1.60 -0.48; BTBT×9 yday $1.53 → 09:30 $1.53 +0.00; ENHA×9 yday $1.66 → 09:30 $1.63 -0.27; QDEL×1 yday $14.49 → 09:30 $15.09 +0.60; ORBS×18 yday $0.84 → 09:30 $0.80 -0.72; BMEA×758 yday $1.61 → 09:30 $1.75 +106.12; NPWR×614 yday $2.02 → 09:30 $1.93 -55.26; PUSA×332 yday $3.91 → 09:30 $3.84 -23.24; ALVO×235 yday $5.25 → 09:30 $4.98 -63.45; CAPR×180 yday $7.19 → 09:30 $8.29 +198.00; ALIT×82 yday $14.87 → 09:30 $14.85 -1.64; ZURA×192 yday $6.50 → 09:30 $6.13 -71.04; SAFX×3238 yday $0.37 → 09:30 $0.35 -64.76 |
| 2026-08-28 | +0.75 | $92.07 | BMEA×758, NPWR×614, PUSA×332, ALVO×235, CAPR×180, ALIT×82, ZURA×192, SAFX×3238 | $10,228.07 | +23.67 | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | BMEA, NPWR, PUSA, ALVO, ALIT, ZURA, SAFX | $93.20 | $10,120.00 | CAPR×180, ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | 09:30 open · cash $92.07 (unchanged overnight, no fees) · equity $10,228.07 vs prior close $10,204.40 (+23.67) because holdings re-marked: BMEA×758 yday $1.71 → 09:30 $1.74 +22.74; NPWR×614 yday $1.81 → 09:30 $1.83 +12.28; PUSA×332 yday $3.85 → 09:30 $3.86 +3.32; ALVO×235 yday $4.91 → 09:30 $4.88 -7.05; CAPR×180 yday $9.36 → 09:30 $9.19 -30.60; ALIT×82 yday $14.33 → 09:30 $14.54 +17.22; ZURA×192 yday $5.99 → 09:30 $6.02 +5.76; SAFX×3238 yday $0.39 → 09:30 $0.39 +0.00 |
| 2026-08-31 | -5.85 | $93.20 | CAPR×180, ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | $9,735.83 | -384.17 | — | CAPR | $1,789.83 | $9,688.09 | ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | 09:30 open · cash $93.20 (unchanged overnight, no fees) · equity $9,735.83 vs prior close $10,120.00 (-384.17) because holdings re-marked: CAPR×180 yday $10.06 → 09:30 $9.44 -111.60; ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×71 yday $16.12 → 09:30 $15.44 -48.28; BZ×65 yday $18.00 → 09:30 $17.89 -7.15; LVWR×882 yday $1.36 → 09:30 $1.37 +8.82; SEDG×36 yday $33.51 → 09:30 $31.50 -72.36; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×76 yday $15.66 → 09:30 $14.32 -101.84 |
| 2026-09-01 | -6.30 | $1,789.83 | ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | $9,588.55 | -99.54 | — | — | $1,789.83 | $9,493.19 | ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | 09:30 open · cash $1,789.83 (unchanged overnight, no fees) · equity $9,588.55 vs prior close $9,688.09 (-99.54) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BHVN×71 yday $15.40 → 09:30 $15.45 +3.55; BZ×65 yday $17.90 → 09:30 $17.37 -34.45; LVWR×882 yday $1.34 → 09:30 $1.22 -105.84; SEDG×36 yday $31.27 → 09:30 $32.22 +34.20; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×76 yday $14.20 → 09:30 $15.05 +64.60 |
| 2026-09-02 | -3.83 | $1,789.83 | ANF×8, BHVN×71, BZ×65, LVWR×882, SEDG×36, SMTC×8, GRRR×76 | $9,481.31 | -11.88 | — | ANF, BHVN, BZ, LVWR, SEDG, SMTC, GRRR | $9,456.91 | $9,456.91 | — | 09:30 open · cash $1,789.83 (unchanged overnight, no fees) · equity $9,481.31 vs prior close $9,493.19 (-11.88) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BHVN×71 yday $15.45 → 09:30 $15.39 -4.26; BZ×65 yday $17.17 → 09:30 $17.29 +7.80; LVWR×882 yday $1.18 → 09:30 $1.19 +8.82; SEDG×36 yday $31.80 → 09:30 $31.87 +2.52; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×76 yday $14.80 → 09:30 $14.75 -3.80 |
| 2026-09-03 | -0.90 | $9,456.91 | — | $9,456.91 | +0.00 | GPRO, FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | — | $52.26 | $9,869.39 | GPRO×968, FRVO×64, CRK×75, MMED×51, CTMX×317, SLN×80, EIX×20, CRDL×547 | 09:30 open · cash $9,456.91 · no holdings · equity $9,456.91 vs prior close $9,456.91 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $52.26 | GPRO×968, FRVO×64, CRK×75, MMED×51, CTMX×317, SLN×80, EIX×20, CRDL×547 | $9,992.48 | +123.09 | BAK, EOSE, SLBT, SION | — | $25.53 | $9,489.53 | GPRO×968, FRVO×64, CRK×75, MMED×51, CTMX×317, SLN×80, EIX×20, CRDL×547, BAK×3, EOSE×2, SLBT×2, SION×1 | 09:30 open · cash $52.26 (unchanged overnight, no fees) · equity $9,992.48 vs prior close $9,869.39 (+123.09) because holdings re-marked: GPRO×968 yday $1.69 → 09:30 $1.78 +87.12; FRVO×64 yday $17.98 → 09:30 $18.27 +18.56; CRK×75 yday $15.54 → 09:30 $15.45 -6.75; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; CTMX×317 yday $3.72 → 09:30 $3.73 +3.17; SLN×80 yday $14.79 → 09:30 $14.85 +4.80; EIX×20 yday $55.19 → 09:30 $55.42 +4.60; CRDL×547 yday $2.17 → 09:30 $2.18 +5.47 |

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
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $1,390.20 | ▲ +131.66 after sell → book $9,585.78; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `WWW` | 60 | $20.08 | $2.19 | $-35.56 | $2,592.81 | ▼ -35.56 after sell → book $9,583.59; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $3,746.02 | ▼ -100.46 after sell → book $9,579.67; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $4,977.36 | ▼ -3.75 after sell → book $9,577.47; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `OMER` | 72 | $17.13 | $2.23 | $-20.27 | $6,208.49 | ▼ -20.27 after sell → book $9,575.24; vs 09:30 mark -2.23 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $7,225.34 | ▼ -230.92 after sell → book $9,572.89; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $8,407.11 | ▼ -72.38 after sell → book $9,566.82; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `MXCT` | 899 | $1.29 | $11.76 | $-113.25 | $9,555.06 | ▼ -113.25 after sell → book $9,555.06; vs 09:30 mark -11.76 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,555.06 | ▲ 09:30 equity $9,555.06 vs yday $9,555.06 (+0.00) | 09:30 open · cash $9,555.06 · no holdings · equity $9,555.06 vs prior close $9,555.06 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 57 | $20.65 | $2.16 | — | $8,375.85 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 161 | $7.38 | $2.47 | — | $7,185.20 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 160 | $7.45 | $2.47 | — | $5,990.73 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1194.38 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $4,856.41 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1194.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 110 | $10.77 | $2.32 | — | $3,669.39 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 126 | $9.46 | $2.37 | — | $2,475.06 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 142 | $8.38 | $2.42 | — | $1,282.68 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1194.38 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 40 | $29.20 | $2.11 | — | $112.57 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1194.38 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $112.57 | ▲ 09:30 equity $10,014.82 vs yday $9,686.98 (+327.84) | 09:30 open · cash $112.57 (unchanged overnight, no fees) · equity $10,014.82 vs prior close $9,686.98 (+327.84) because holdings re-marked: CDE×57 yday $21.11 → 09:30 $21.75 +36.48; MRVI×161 yday $8.26 → 09:30 $8.20 -9.66; DNA×160 yday $6.96 → 09:30 $7.09 +20.80; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×110 yday $10.97 → 09:30 $11.34 +40.70; SCZM×126 yday $9.76 → 09:30 $10.26 +63.00; NG×142 yday $8.66 → 09:30 $9.02 +51.12; BLSH×40 yday $28.44 → 09:30 $29.75 +52.40 | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $101.33 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $16.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 12 | $1.32 | $0.19 | — | $85.29 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $16.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 9 | $1.66 | $0.18 | — | $70.18 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $16.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 9 | $1.71 | $0.18 | — | $54.61 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $16.08 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 1 | $14.96 | $0.15 | — | $39.49 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $16.08 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 18 | $0.86 | $0.21 | — | $23.73 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $16.08 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.73 | ▲ 09:30 equity $10,011.30 vs yday $9,928.43 (+82.87) | 09:30 open · cash $23.73 (unchanged overnight, no fees) · equity $10,011.30 vs prior close $9,928.43 (+82.87) because holdings re-marked: CDE×57 yday $20.97 → 09:30 $21.26 +16.53; MRVI×161 yday $8.70 → 09:30 $8.59 -17.71; DNA×160 yday $7.40 → 09:30 $7.26 -22.40; MSTR×10 yday $119.25 → 09:30 $121.76 +25.10; EXK×110 yday $10.62 → 09:30 $11.01 +42.90; SCZM×126 yday $9.68 → 09:30 $9.82 +18.27; NG×142 yday $8.72 → 09:30 $8.89 +24.14; BLSH×40 yday $30.41 → 09:30 $30.18 -9.20; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; CYPH×12 yday $1.42 → 09:30 $1.83 +4.92; BTBT×9 yday $1.53 → 09:30 $1.55 +0.18; ENHA×9 yday $1.72 → 09:30 $1.74 +0.18; QDEL×1 yday $14.74 → 09:30 $14.71 -0.03; ORBS×18 yday $0.88 → 09:30 $0.89 +0.18 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $23.73 | ▲ 09:30 equity $9,941.52 vs yday $9,906.03 (+35.49) | 09:30 open · cash $23.73 (unchanged overnight, no fees) · equity $9,941.52 vs prior close $9,906.03 (+35.49) because holdings re-marked: CDE×57 yday $20.49 → 09:30 $20.85 +20.52; MRVI×161 yday $8.26 → 09:30 $8.31 +8.05; DNA×160 yday $6.98 → 09:30 $6.82 -25.60; MSTR×10 yday $124.59 → 09:30 $125.56 +9.70; EXK×110 yday $10.74 → 09:30 $10.72 -2.20; SCZM×126 yday $9.53 → 09:30 $9.57 +5.04; NG×142 yday $9.24 → 09:30 $9.34 +14.20; BLSH×40 yday $30.88 → 09:30 $31.00 +4.80; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; CYPH×12 yday $1.64 → 09:30 $1.70 +0.72; BTBT×9 yday $1.56 → 09:30 $1.55 -0.09; ENHA×9 yday $1.69 → 09:30 $1.65 -0.36; QDEL×1 yday $14.36 → 09:30 $14.49 +0.13; ORBS×18 yday $0.85 → 09:30 $0.85 +0.00 | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 57 | $20.85 | $2.18 | $+7.06 | $1,210.00 | ▲ +7.06 after sell → book $9,939.34; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MRVI` | 161 | $8.31 | $2.51 | $+144.75 | $2,545.40 | ▲ +144.75 after sell → book $9,936.83; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `DNA` | 160 | $6.82 | $2.51 | $-105.78 | $3,634.09 | ▼ -105.78 after sell → book $9,934.32; vs 09:30 mark -2.51 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `MSTR` | 10 | $125.56 | $2.04 | $+119.24 | $4,887.65 | ▲ +119.24 after sell → book $9,932.28; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `EXK` | 110 | $10.72 | $2.35 | $-10.17 | $6,064.51 | ▼ -10.17 after sell → book $9,929.94; vs 09:30 mark -2.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `SCZM` | 126 | $9.57 | $2.40 | $+9.09 | $7,267.93 | ▲ +9.09 after sell → book $9,927.54; vs 09:30 mark -2.40 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NG` | 142 | $9.34 | $2.45 | $+131.45 | $8,591.76 | ▲ +131.45 after sell → book $9,925.09; vs 09:30 mark -2.45 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BLSH` | 40 | $31.00 | $2.13 | $+67.76 | $9,829.63 | ▲ +67.76 after sell → book $9,922.96; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 758 | $1.62 | $9.78 | — | $8,591.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1228.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 614 | $2.00 | $7.92 | — | $7,355.97 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1228.70 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 332 | $3.70 | $4.28 | — | $6,123.29 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1228.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 235 | $5.22 | $3.03 | — | $4,893.55 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1228.70 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 180 | $6.79 | $2.53 | — | $3,668.82 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1228.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 82 | $14.86 | $2.24 | — | $2,448.07 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1228.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 192 | $6.38 | $2.57 | — | $1,220.54 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1228.70 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3238 | $0.37 | $21.69 | — | $0.79 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $1228.70 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.79 | ▲ 09:30 equity $10,045.13 vs yday $10,045.13 (-0.00) | 09:30 open · cash $0.79 (unchanged overnight, no fees) · equity $10,045.13 vs prior close $10,045.13 (-0.00) because holdings re-marked: ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; CYPH×12 yday $1.64 → 09:30 $1.64 +0.00; BTBT×9 yday $1.53 → 09:30 $1.53 +0.00; ENHA×9 yday $1.66 → 09:30 $1.66 +0.00; QDEL×1 yday $14.49 → 09:30 $14.49 +0.00; ORBS×18 yday $0.84 → 09:30 $0.84 +0.00; BMEA×758 yday $1.61 → 09:30 $1.61 +0.00; NPWR×614 yday $2.02 → 09:30 $2.02 +0.00; PUSA×332 yday $3.91 → 09:30 $3.91 +0.00; ALVO×235 yday $5.25 → 09:30 $5.25 +0.00; CAPR×180 yday $7.19 → 09:30 $7.19 +0.00; ALIT×82 yday $14.87 → 09:30 $14.87 +0.00; ZURA×192 yday $6.50 → 09:30 $6.50 +0.00; SAFX×3238 yday $0.37 → 09:30 $0.37 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.79 | ▲ 09:30 equity $10,070.13 vs yday $9,868.92 (+201.21) | 09:30 open · cash $0.79 (unchanged overnight, no fees) · equity $10,070.13 vs prior close $9,868.92 (+201.21) because holdings re-marked: ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; CYPH×12 yday $1.64 → 09:30 $1.60 -0.48; BTBT×9 yday $1.53 → 09:30 $1.53 +0.00; ENHA×9 yday $1.66 → 09:30 $1.63 -0.27; QDEL×1 yday $14.49 → 09:30 $15.09 +0.60; ORBS×18 yday $0.84 → 09:30 $0.80 -0.72; BMEA×758 yday $1.61 → 09:30 $1.75 +106.12; NPWR×614 yday $2.02 → 09:30 $1.93 -55.26; PUSA×332 yday $3.91 → 09:30 $3.84 -23.24; ALVO×235 yday $5.25 → 09:30 $4.98 -63.45; CAPR×180 yday $7.19 → 09:30 $8.29 +198.00; ALIT×82 yday $14.87 → 09:30 $14.85 -1.64; ZURA×192 yday $6.50 → 09:30 $6.13 -71.04; SAFX×3238 yday $0.37 → 09:30 $0.35 -64.76 | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $15.96 | ▲ +3.93 after sell → book $10,069.95; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 12 | $1.60 | $0.25 | $+2.92 | $34.91 | ▲ +2.92 after sell → book $10,069.70; vs 09:30 mark -0.25 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 9 | $1.53 | $0.18 | $-1.53 | $48.50 | ▼ -1.53 after sell → book $10,069.52; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ENHA` | 9 | $1.63 | $0.19 | $-1.09 | $62.97 | ▼ -1.09 after sell → book $10,069.32; vs 09:30 mark -0.20 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `QDEL` | 1 | $15.09 | $0.17 | $-0.20 | $77.89 | ▼ -0.20 after sell → book $10,069.15; vs 09:30 mark -0.17 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ORBS` | 18 | $0.80 | $0.22 | $-1.58 | $92.07 | ▼ -1.58 after sell → book $10,068.93; vs 09:30 mark -0.22 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $92.07 | ▲ 09:30 equity $10,228.07 vs yday $10,204.40 (+23.67) | 09:30 open · cash $92.07 (unchanged overnight, no fees) · equity $10,228.07 vs prior close $10,204.40 (+23.67) because holdings re-marked: BMEA×758 yday $1.71 → 09:30 $1.74 +22.74; NPWR×614 yday $1.81 → 09:30 $1.83 +12.28; PUSA×332 yday $3.85 → 09:30 $3.86 +3.32; ALVO×235 yday $4.91 → 09:30 $4.88 -7.05; CAPR×180 yday $9.36 → 09:30 $9.19 -30.60; ALIT×82 yday $14.33 → 09:30 $14.54 +17.22; ZURA×192 yday $5.99 → 09:30 $6.02 +5.76; SAFX×3238 yday $0.39 → 09:30 $0.39 +0.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 758 | $1.74 | $9.91 | $+71.27 | $1,401.08 | ▲ +71.27 after sell → book $10,218.16; vs 09:30 mark -9.91 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 614 | $1.83 | $8.03 | $-120.33 | $2,516.67 | ▼ -120.33 after sell → book $10,210.13; vs 09:30 mark -8.03 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 332 | $3.86 | $4.35 | $+44.49 | $3,793.84 | ▲ +44.49 after sell → book $10,205.78; vs 09:30 mark -4.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 235 | $4.88 | $3.08 | $-86.01 | $4,937.56 | ▼ -86.01 after sell → book $10,202.70; vs 09:30 mark -3.08 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALIT` | 82 | $14.54 | $2.26 | $-30.74 | $6,127.58 | ▼ -30.74 after sell → book $10,200.44; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 192 | $6.02 | $2.61 | $-74.29 | $7,280.81 | ▼ -74.29 after sell → book $10,197.83; vs 09:30 mark -2.61 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SAFX` | 3238 | $0.39 | $22.89 | $+20.18 | $8,520.74 | ▲ +20.18 after sell → book $10,174.94; vs 09:30 mark -22.89 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $7,361.13 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1217.25 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 71 | $16.95 | $2.20 | — | $6,155.47 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1217.25 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 65 | $18.50 | $2.19 | — | $4,950.79 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1217.25 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 882 | $1.38 | $11.38 | — | $3,722.25 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1217.25 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 36 | $33.78 | $2.10 | — | $2,504.07 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1217.25 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 8 | $149.40 | $2.01 | — | $1,306.86 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1217.25 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 76 | $15.94 | $2.22 | — | $93.20 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1217.25 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $93.20 | ▼ 09:30 equity $9,735.83 vs yday $10,120.00 (-384.17) | 09:30 open · cash $93.20 (unchanged overnight, no fees) · equity $9,735.83 vs prior close $10,120.00 (-384.17) because holdings re-marked: CAPR×180 yday $10.06 → 09:30 $9.44 -111.60; ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×71 yday $16.12 → 09:30 $15.44 -48.28; BZ×65 yday $18.00 → 09:30 $17.89 -7.15; LVWR×882 yday $1.36 → 09:30 $1.37 +8.82; SEDG×36 yday $33.51 → 09:30 $31.50 -72.36; SMTC×8 yday $142.43 → 09:30 $133.04 -75.12; GRRR×76 yday $15.66 → 09:30 $14.32 -101.84 | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 180 | $9.44 | $2.57 | $+471.90 | $1,789.83 | ▲ +471.90 after sell → book $9,733.26; vs 09:30 mark -2.57 | dropped from list after 4 sess (min 3) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,789.83 | ▼ 09:30 equity $9,588.55 vs yday $9,688.09 (-99.54) | 09:30 open · cash $1,789.83 (unchanged overnight, no fees) · equity $9,588.55 vs prior close $9,688.09 (-99.54) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BHVN×71 yday $15.40 → 09:30 $15.45 +3.55; BZ×65 yday $17.90 → 09:30 $17.37 -34.45; LVWR×882 yday $1.34 → 09:30 $1.22 -105.84; SEDG×36 yday $31.27 → 09:30 $32.22 +34.20; SMTC×8 yday $132.54 → 09:30 $131.65 -7.12; GRRR×76 yday $14.20 → 09:30 $15.05 +64.60 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1,789.83 | ▼ 09:30 equity $9,481.31 vs yday $9,493.19 (-11.88) | 09:30 open · cash $1,789.83 (unchanged overnight, no fees) · equity $9,481.31 vs prior close $9,493.19 (-11.88) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BHVN×71 yday $15.45 → 09:30 $15.39 -4.26; BZ×65 yday $17.17 → 09:30 $17.29 +7.80; LVWR×882 yday $1.18 → 09:30 $1.19 +8.82; SEDG×36 yday $31.80 → 09:30 $31.87 +2.52; SMTC×8 yday $129.50 → 09:30 $127.63 -14.96; GRRR×76 yday $14.80 → 09:30 $14.75 -3.80 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $142.00 | $2.03 | $-25.65 | $2,923.79 | ▼ -25.65 after sell → book $9,479.27; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 71 | $15.39 | $2.22 | $-115.19 | $4,014.26 | ▼ -115.19 after sell → book $9,477.05; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 65 | $17.29 | $2.21 | $-83.04 | $5,135.90 | ▼ -83.04 after sell → book $9,474.84; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `LVWR` | 882 | $1.19 | $11.53 | $-190.49 | $6,173.95 | ▼ -190.49 after sell → book $9,463.31; vs 09:30 mark -11.53 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 36 | $31.87 | $2.12 | $-72.98 | $7,319.15 | ▼ -72.98 after sell → book $9,461.19; vs 09:30 mark -2.12 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 8 | $127.63 | $2.03 | $-178.21 | $8,338.16 | ▼ -178.21 after sell → book $9,459.16; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `GRRR` | 76 | $14.75 | $2.24 | $-94.90 | $9,456.91 | ▼ -94.90 after sell → book $9,456.91; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,456.91 | ▲ 09:30 equity $9,456.91 vs yday $9,456.91 (+0.00) | 09:30 open · cash $9,456.91 · no holdings · equity $9,456.91 vs prior close $9,456.91 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 968 | $1.22 | $12.49 | — | $8,263.47 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1182.11 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 64 | $18.40 | $2.18 | — | $7,083.69 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1182.11 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 75 | $15.70 | $2.21 | — | $5,903.97 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1182.11 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $4,740.05 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1182.11 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 317 | $3.72 | $4.09 | — | $3,556.72 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1182.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 80 | $14.70 | $2.23 | — | $2,378.49 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1182.11 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 20 | $56.78 | $2.05 | — | $1,240.84 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1182.11 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 547 | $2.16 | $7.06 | — | $52.26 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1182.11 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $52.26 | ▲ 09:30 equity $9,992.48 vs yday $9,869.39 (+123.09) | 09:30 open · cash $52.26 (unchanged overnight, no fees) · equity $9,992.48 vs prior close $9,869.39 (+123.09) because holdings re-marked: GPRO×968 yday $1.69 → 09:30 $1.78 +87.12; FRVO×64 yday $17.98 → 09:30 $18.27 +18.56; CRK×75 yday $15.54 → 09:30 $15.45 -6.75; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; CTMX×317 yday $3.72 → 09:30 $3.73 +3.17; SLN×80 yday $14.79 → 09:30 $14.85 +4.80; EIX×20 yday $55.19 → 09:30 $55.42 +4.60; CRDL×547 yday $2.17 → 09:30 $2.18 +5.47 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 3 | $1.95 | $0.07 | — | $46.34 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7.47 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 2 | $3.57 | $0.08 | — | $39.13 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $7.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 2 | $3.07 | $0.07 | — | $32.92 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $7.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 1 | $7.31 | $0.08 | — | $25.53 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $7.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `WWW` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `OMER` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `MXCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 0.61 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 0.61 < 1 share @ 9.12 |
| 2026-08-17 | `FCEL` | cash | leftover split 0.61 < 1 share @ 22.37 |
| 2026-08-17 | `VERA` | cash | leftover split 0.61 < 1 share @ 31.30 |
| 2026-08-17 | `CELC` | cash | leftover split 0.61 < 1 share @ 92.99 |
| 2026-08-17 | `CAPR` | cash | leftover split 0.61 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 0.61 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 0.61 < 1 share @ 32.55 |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `WWW` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `OMER` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `MXCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `JLHL` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `CBRS` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `COHR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `TDTH` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PGEN` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `INDP` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `PURR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `GUTS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOL` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `ULTA` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `MSTR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `EXK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `SCZM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BLSH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `DE` | cash | leftover split 16.08 < 1 share @ 623.26 |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MRVI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `DNA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `MSTR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `EXK` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `SCZM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BLSH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ENHA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `QDEL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ORBS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CYPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ENHA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `QDEL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ORBS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ENHA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `QDEL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ORBS` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALIT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SAFX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `RZLT` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
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
| 2026-09-04 | `DELL` | cash | leftover split 7.47 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 7.47 < 1 share @ 29.15 |
| 2026-09-04 | `CCOI` | cash | leftover split 7.47 < 1 share @ 10.22 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 968 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1182.11 |
| `FRVO` | 64 | 2026-09-03 @ $18.40 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1182.11 |
| `CRK` | 75 | 2026-09-03 @ $15.70 | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1182.11 |
| `MMED` | 51 | 2026-09-03 @ $22.78 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1182.11 |
| `CTMX` | 317 | 2026-09-03 @ $3.72 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1182.11 |
| `SLN` | 80 | 2026-09-03 @ $14.70 | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1182.11 |
| `EIX` | 20 | 2026-09-03 @ $56.78 | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1182.11 |
| `CRDL` | 547 | 2026-09-03 @ $2.16 | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1182.11 |
| `BAK` | 3 | 2026-09-04 @ $1.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $7.47 |
| `EOSE` | 2 | 2026-09-04 @ $3.57 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $7.47 |
| `SLBT` | 2 | 2026-09-04 @ $3.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $7.47 |
| `SION` | 1 | 2026-09-04 @ $7.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $7.47 |
