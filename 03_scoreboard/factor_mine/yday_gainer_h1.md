# Factor mine action — `yday_gainer_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `yday_gainer` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · baseline list, no extra gate

Cash book **+4.56%** ($10,456) · signal-only (no cash/fees) was +5.63%. Starts YES **11/17**. Fills 116 · skips 62 · realized $+422.04.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `yday_gainer` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `none (list as ranked)` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $332.47.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | — | $4.90 | $9,804.72 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $4.90 | ANGX×290, WWW×60, HYLN×299, ARX×63, OMER×72, AIRO×112, NCMI×464, MXCT×899 | $9,850.47 | +45.75 | CDNL, ABX, FCEL, VERA, CELC, CAPR, HTFL, UMAC | ANGX, WWW, HYLN, ARX, OMER, AIRO, NCMI, MXCT | $120.48 | $9,820.10 | CDNL×30, ABX×134, FCEL×54, VERA×39, CELC×13, CAPR×178, HTFL×29, UMAC×37 | 09:30 open · cash $4.90 (unchanged overnight, no fees) · equity $9,850.47 vs prior close $9,804.72 (+45.75) because holdings re-marked: ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; WWW×60 yday $21.03 → 09:30 $20.98 -3.00; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; OMER×72 yday $17.19 → 09:30 $17.17 -1.44; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; MXCT×899 yday $1.32 → 09:30 $1.32 +0.00 |
| 2026-08-18 | -6.20 | $120.48 | CDNL×30, ABX×134, FCEL×54, VERA×39, CELC×13, CAPR×178, HTFL×29, UMAC×37 | $9,739.68 | -80.42 | — | CDNL, ABX, FCEL, VERA, CELC, CAPR, HTFL, UMAC | $9,722.02 | $9,722.02 | — | 09:30 open · cash $120.48 (unchanged overnight, no fees) · equity $9,739.68 vs prior close $9,820.10 (-80.42) because holdings re-marked: CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×134 yday $9.12 → 09:30 $9.03 -12.06; FCEL×54 yday $22.36 → 09:30 $21.18 -63.72; VERA×39 yday $31.63 → 09:30 $31.31 -12.48; CELC×13 yday $92.44 → 09:30 $92.38 -0.78; CAPR×178 yday $7.45 → 09:30 $7.50 +8.90; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72 |
| 2026-08-19 | -7.20 | $9,722.02 | — | $9,722.02 | +0.00 | — | — | $9,722.02 | $9,722.02 | — | 09:30 open · cash $9,722.02 · no holdings · equity $9,722.02 vs prior close $9,722.02 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $9,722.02 | — | $9,722.02 | +0.00 | CDE, MRVI, DNA, MSTR, EXK, SCZM, NG, BLSH | — | $119.55 | $9,856.61 | CDE×58, MRVI×164, DNA×163, MSTR×10, EXK×112, SCZM×128, NG×145, BLSH×41 | 09:30 open · cash $9,722.02 · no holdings · equity $9,722.02 vs prior close $9,722.02 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $119.55 | CDE×58, MRVI×164, DNA×163, MSTR×10, EXK×112, SCZM×128, NG×145, BLSH×41 | $10,189.43 | +332.82 | ARCT, CYPH, BTBT, ENHA, DE, QDEL, ORBS | CDE, DNA, MSTR, EXK, SCZM, NG, BLSH | $0.91 | $10,519.67 | MRVI×164, ARCT×113, CYPH×955, BTBT×759, ENHA×737, DE×2, QDEL×84, ORBS×1425 | 09:30 open · cash $119.55 (unchanged overnight, no fees) · equity $10,189.43 vs prior close $9,856.61 (+332.82) because holdings re-marked: CDE×58 yday $21.11 → 09:30 $21.75 +37.12; MRVI×164 yday $8.26 → 09:30 $8.20 -9.84; DNA×163 yday $6.96 → 09:30 $7.09 +21.19; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×112 yday $10.97 → 09:30 $11.34 +41.44; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00; NG×145 yday $8.66 → 09:30 $9.02 +52.20; BLSH×41 yday $28.44 → 09:30 $29.75 +53.71 |
| 2026-08-24 | -5.17 | $0.91 | MRVI×164, ARCT×113, CYPH×955, BTBT×759, ENHA×737, DE×2, QDEL×84, ORBS×1425 | $10,925.66 | +405.99 | — | MRVI, ARCT, CYPH, BTBT, ENHA, DE, QDEL, ORBS | $10,867.23 | $10,867.23 | — | 09:30 open · cash $0.91 (unchanged overnight, no fees) · equity $10,925.66 vs prior close $10,519.67 (+405.99) because holdings re-marked: MRVI×164 yday $8.70 → 09:30 $8.59 -18.04; ARCT×113 yday $13.45 → 09:30 $13.26 -21.47; CYPH×955 yday $1.42 → 09:30 $1.83 +391.55; BTBT×759 yday $1.53 → 09:30 $1.55 +15.18; ENHA×737 yday $1.72 → 09:30 $1.74 +14.74; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×84 yday $14.74 → 09:30 $14.71 -2.52; ORBS×1425 yday $0.88 → 09:30 $0.89 +14.25 |
| 2026-08-25 | +1.80 | $10,867.23 | — | $10,867.23 | -0.00 | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | — | $0.84 | $11,004.61 | BMEA×838, NPWR×679, PUSA×367, ALVO×260, CAPR×200, ALIT×91, ZURA×212, SAFX×3551 | 09:30 open · cash $10,867.23 · no holdings · equity $10,867.23 vs prior close $10,867.23 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $0.84 | BMEA×838, NPWR×679, PUSA×367, ALVO×260, CAPR×200, ALIT×91, ZURA×212, SAFX×3551 | $11,004.61 | +0.00 | — | — | $0.84 | $10,808.19 | BMEA×838, NPWR×679, PUSA×367, ALVO×260, CAPR×200, ALIT×91, ZURA×212, SAFX×3551 | 09:30 open · cash $0.84 (unchanged overnight, no fees) · equity $11,004.61 vs prior close $11,004.61 (+0.00) because holdings re-marked: BMEA×838 yday $1.61 → 09:30 $1.61 +0.00; NPWR×679 yday $2.02 → 09:30 $2.02 +0.00; PUSA×367 yday $3.91 → 09:30 $3.91 +0.00; ALVO×260 yday $5.25 → 09:30 $5.25 +0.00; CAPR×200 yday $7.19 → 09:30 $7.19 +0.00; ALIT×91 yday $14.87 → 09:30 $14.87 +0.00; ZURA×212 yday $6.50 → 09:30 $6.50 +0.00; SAFX×3551 yday $0.37 → 09:30 $0.37 +0.00 |
| 2026-08-27 | — | $0.84 | BMEA×838, NPWR×679, PUSA×367, ALVO×260, CAPR×200, ALIT×91, ZURA×212, SAFX×3551 | $11,033.65 | +225.46 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ALIT, ZURA, SAFX | $10,974.21 | $10,974.21 | — | 09:30 open · cash $0.84 (unchanged overnight, no fees) · equity $11,033.65 vs prior close $10,808.19 (+225.46) because holdings re-marked: BMEA×838 yday $1.61 → 09:30 $1.75 +117.32; NPWR×679 yday $2.02 → 09:30 $1.93 -61.11; PUSA×367 yday $3.91 → 09:30 $3.84 -25.69; ALVO×260 yday $5.25 → 09:30 $4.98 -70.20; CAPR×200 yday $7.19 → 09:30 $8.29 +220.00; ALIT×91 yday $14.87 → 09:30 $14.85 -1.82; ZURA×212 yday $6.50 → 09:30 $6.13 -78.44; SAFX×3551 yday $0.37 → 09:30 $0.35 -71.02 |
| 2026-08-28 | +0.75 | $10,974.21 | — | $10,974.21 | -0.00 | ANF, BHVN, BZ, CAPR, LVWR, SEDG, SMTC, GRRR | — | $111.15 | $10,864.31 | ANF×9, BHVN×80, BZ×74, CAPR×149, LVWR×994, SEDG×40, SMTC×9, GRRR×86 | 09:30 open · cash $10,974.21 · no holdings · equity $10,974.21 vs prior close $10,974.21 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $111.15 | ANF×9, BHVN×80, BZ×74, CAPR×149, LVWR×994, SEDG×40, SMTC×9, GRRR×86 | $10,465.46 | -398.85 | — | ANF, BHVN, BZ, CAPR, LVWR, SEDG, SMTC, GRRR | $10,437.02 | $10,437.02 | — | 09:30 open · cash $111.15 (unchanged overnight, no fees) · equity $10,465.46 vs prior close $10,864.31 (-398.85) because holdings re-marked: ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×80 yday $16.12 → 09:30 $15.44 -54.40; BZ×74 yday $18.00 → 09:30 $17.89 -8.14; CAPR×149 yday $10.06 → 09:30 $9.44 -92.38; LVWR×994 yday $1.36 → 09:30 $1.37 +9.94; SEDG×40 yday $33.51 → 09:30 $31.50 -80.40; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; GRRR×86 yday $15.66 → 09:30 $14.32 -115.24 |
| 2026-09-01 | -6.30 | $10,437.02 | — | $10,437.02 | -0.00 | — | — | $10,437.02 | $10,437.02 | — | 09:30 open · cash $10,437.02 · no holdings · equity $10,437.02 vs prior close $10,437.02 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $10,437.02 | — | $10,437.02 | -0.00 | — | — | $10,437.02 | $10,437.02 | — | 09:30 open · cash $10,437.02 · no holdings · equity $10,437.02 vs prior close $10,437.02 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $10,437.02 | — | $10,437.02 | -0.00 | GPRO, FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | — | $59.04 | $10,894.60 | GPRO×1069, FRVO×70, CRK×83, MMED×57, CTMX×350, SLN×88, EIX×22, CRDL×603 | 09:30 open · cash $10,437.02 · no holdings · equity $10,437.02 vs prior close $10,437.02 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $59.04 | GPRO×1069, FRVO×70, CRK×83, MMED×57, CTMX×350, SLN×88, EIX×22, CRDL×603 | $11,030.35 | +135.75 | BAK, EOSE, SLBT, DELL, MLYS, CCOI, SION | FRVO, CRK, MMED, CTMX, SLN, EIX, CRDL | $332.47 | $10,455.74 | GPRO×1069, BAK×666, EOSE×364, SLBT×423, DELL×2, MLYS×44, CCOI×127, SION×177 | 09:30 open · cash $59.04 (unchanged overnight, no fees) · equity $11,030.35 vs prior close $10,894.60 (+135.75) because holdings re-marked: GPRO×1069 yday $1.69 → 09:30 $1.78 +96.21; FRVO×70 yday $17.98 → 09:30 $18.27 +20.30; CRK×83 yday $15.54 → 09:30 $15.45 -7.47; MMED×57 yday $23.76 → 09:30 $23.88 +6.84; CTMX×350 yday $3.72 → 09:30 $3.73 +3.50; SLN×88 yday $14.79 → 09:30 $14.85 +5.28; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×603 yday $2.17 → 09:30 $2.18 +6.03 |

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
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $1,335.10 | ▲ +76.56 after sell → book $9,846.67; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `WWW` | 60 | $20.98 | $2.19 | $+18.44 | $2,591.71 | ▲ +18.44 after sell → book $9,844.48; vs 09:30 mark -2.19 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $3,813.69 | ▼ -31.69 after sell → book $9,840.56; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $5,044.40 | ▼ -4.38 after sell → book $9,838.36; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `OMER` | 72 | $17.17 | $2.23 | $-17.39 | $6,278.41 | ▼ -17.39 after sell → book $9,836.13; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,347.90 | ▼ -178.28 after sell → book $9,833.78; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,641.03 | ▲ +38.98 after sell → book $9,827.71; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `MXCT` | 899 | $1.32 | $11.76 | $-86.28 | $9,815.95 | ▼ -86.28 after sell → book $9,815.95; vs 09:30 mark -11.76 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $8,618.37 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 134 | $9.12 | $2.39 | — | $7,393.90 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1226.99 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `FCEL` | 54 | $22.37 | $2.15 | — | $6,183.77 | — | baseline list, no extra gate; list probable,yday_gainer; ⚪; ret5=+9.5; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 39 | $31.30 | $2.11 | — | $4,960.96 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-3.8; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CELC` | 13 | $92.99 | $2.03 | — | $3,750.06 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-0.8; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟢 judge🟢 vol🟡 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `CAPR` | 178 | $6.87 | $2.52 | — | $2,524.68 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+62.6; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $1,326.93 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ret5=+46.0; leftover $1226.99 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $120.48 | — | baseline list, no extra gate; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1226.99 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $120.48 | ▼ 09:30 equity $9,739.68 vs yday $9,820.10 (-80.42) | 09:30 open · cash $120.48 (unchanged overnight, no fees) · equity $9,739.68 vs prior close $9,820.10 (-80.42) because holdings re-marked: CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×134 yday $9.12 → 09:30 $9.03 -12.06; FCEL×54 yday $22.36 → 09:30 $21.18 -63.72; VERA×39 yday $31.63 → 09:30 $31.31 -12.48; CELC×13 yday $92.44 → 09:30 $92.38 -0.78; CAPR×178 yday $7.45 → 09:30 $7.50 +8.90; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $1,365.48 | ▲ +47.42 after sell → book $9,737.58; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 134 | $9.03 | $2.42 | $-16.88 | $2,573.07 | ▼ -16.88 after sell → book $9,735.15; vs 09:30 mark -2.43 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `FCEL` | 54 | $21.18 | $2.17 | $-68.58 | $3,714.62 | ▼ -68.58 after sell → book $9,732.98; vs 09:30 mark -2.17 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 39 | $31.31 | $2.13 | $-3.84 | $4,933.59 | ▼ -3.84 after sell → book $9,730.86; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CELC` | 13 | $92.38 | $2.05 | $-12.01 | $6,132.48 | ▼ -12.01 after sell → book $9,728.81; vs 09:30 mark -2.05 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `CAPR` | 178 | $7.50 | $2.56 | $+107.05 | $7,464.91 | ▲ +107.05 after sell → book $9,726.24; vs 09:30 mark -2.57 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🔴 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $8,666.31 | ▲ +3.66 after sell → book $9,724.14; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $9,722.02 | ▼ -150.74 after sell → book $9,722.02; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.02 | ▲ 09:30 equity $9,722.02 vs yday $9,722.02 (+0.00) | 09:30 open · cash $9,722.02 · no holdings · equity $9,722.02 vs prior close $9,722.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,722.02 | ▲ 09:30 equity $9,722.02 vs yday $9,722.02 (+0.00) | 09:30 open · cash $9,722.02 · no holdings · equity $9,722.02 vs prior close $9,722.02 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 58 | $20.65 | $2.16 | — | $8,522.16 | — | baseline list, no extra gate; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MRVI` | 164 | $7.38 | $2.48 | — | $7,309.36 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-8.5; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `DNA` | 163 | $7.45 | $2.48 | — | $6,092.53 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+6.9; leftover $1215.25 | join🔴 sector🟡 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `MSTR` | 10 | $113.23 | $2.02 | — | $4,958.21 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.9; leftover $1215.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `EXK` | 112 | $10.77 | $2.33 | — | $3,749.64 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.6; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `SCZM` | 128 | $9.46 | $2.37 | — | $2,536.39 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+7.6; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NG` | 145 | $8.38 | $2.42 | — | $1,318.86 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+7.5; leftover $1215.25 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BLSH` | 41 | $29.20 | $2.11 | — | $119.55 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+9.3; leftover $1215.25 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $119.55 | ▲ 09:30 equity $10,189.43 vs yday $9,856.61 (+332.82) | 09:30 open · cash $119.55 (unchanged overnight, no fees) · equity $10,189.43 vs prior close $9,856.61 (+332.82) because holdings re-marked: CDE×58 yday $21.11 → 09:30 $21.75 +37.12; MRVI×164 yday $8.26 → 09:30 $8.20 -9.84; DNA×163 yday $6.96 → 09:30 $7.09 +21.19; MSTR×10 yday $112.39 → 09:30 $119.69 +73.00; EXK×112 yday $10.97 → 09:30 $11.34 +41.44; SCZM×128 yday $9.76 → 09:30 $10.26 +64.00; NG×145 yday $8.66 → 09:30 $9.02 +52.20; BLSH×41 yday $28.44 → 09:30 $29.75 +53.71 | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 58 | $21.75 | $2.18 | $+59.45 | $1,378.87 | ▲ +59.45 after sell → book $10,187.25; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `DNA` | 163 | $7.09 | $2.52 | $-63.68 | $2,532.02 | ▼ -63.68 after sell → book $10,184.73; vs 09:30 mark -2.52 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `MSTR` | 10 | $119.69 | $2.04 | $+60.54 | $3,726.88 | ▲ +60.54 after sell → book $10,182.69; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🔴 sector🟢 gen🟢 news🟡 digest🟢 judge🔴 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `EXK` | 112 | $11.34 | $2.35 | $+59.16 | $4,994.61 | ▲ +59.16 after sell → book $10,180.34; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `SCZM` | 128 | $10.26 | $2.41 | $+97.62 | $6,305.48 | ▲ +97.62 after sell → book $10,177.93; vs 09:30 mark -2.41 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NG` | 145 | $9.02 | $2.46 | $+87.92 | $7,610.92 | ▲ +87.92 after sell → book $10,175.47; vs 09:30 mark -2.46 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BLSH` | 41 | $29.75 | $2.13 | $+18.30 | $8,828.54 | ▲ +18.30 after sell → book $10,173.34; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 113 | $11.13 | $2.33 | — | $7,568.52 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1261.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 955 | $1.32 | $12.32 | — | $6,295.60 | — | baseline list, no extra gate; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1261.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 759 | $1.66 | $9.79 | — | $5,025.87 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1261.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ENHA` | 737 | $1.71 | $9.51 | — | $3,756.09 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-32.0; leftover $1261.22 | join🔴 sector🔴 gen🟢 news🟡 digest🟡 ab🔴 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 2 | $623.26 | $2.00 | — | $2,507.57 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1261.22 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `QDEL` | 84 | $14.96 | $2.24 | — | $1,248.69 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-1.6; leftover $1261.22 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟡 peer🔴 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ORBS` | 1425 | $0.86 | $16.59 | — | $0.91 | — | baseline list, no extra gate; list probable,yday_gainer,ohlc_hot; ret5=+9.7; leftover $1261.22 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟡 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.91 | ▲ 09:30 equity $10,925.66 vs yday $10,519.67 (+405.99) | 09:30 open · cash $0.91 (unchanged overnight, no fees) · equity $10,925.66 vs prior close $10,519.67 (+405.99) because holdings re-marked: MRVI×164 yday $8.70 → 09:30 $8.59 -18.04; ARCT×113 yday $13.45 → 09:30 $13.26 -21.47; CYPH×955 yday $1.42 → 09:30 $1.83 +391.55; BTBT×759 yday $1.53 → 09:30 $1.55 +15.18; ENHA×737 yday $1.72 → 09:30 $1.74 +14.74; DE×2 yday $647.47 → 09:30 $653.62 +12.30; QDEL×84 yday $14.74 → 09:30 $14.71 -2.52; ORBS×1425 yday $0.88 → 09:30 $0.89 +14.25 | — |
| 2026-08-24 09:30 ET | **SELL** | `MRVI` | 164 | $8.59 | $2.52 | $+193.44 | $1,407.15 | ▲ +193.44 after sell → book $10,923.14; vs 09:30 mark -2.52 | dropped from list after 2 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 113 | $13.26 | $2.36 | $+236.00 | $2,903.17 | ▲ +236.00 after sell → book $10,920.78; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 955 | $1.83 | $12.49 | $+462.24 | $4,638.32 | ▲ +462.24 after sell → book $10,908.28; vs 09:30 mark -12.50 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 759 | $1.55 | $9.93 | $-103.21 | $5,804.85 | ▼ -103.21 after sell → book $10,898.36; vs 09:30 mark -9.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ENHA` | 737 | $1.74 | $9.64 | $+2.96 | $7,077.59 | ▲ +2.96 after sell → book $10,888.72; vs 09:30 mark -9.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 2 | $653.62 | $2.02 | $+56.71 | $8,382.81 | ▲ +56.71 after sell → book $10,886.70; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `QDEL` | 84 | $14.71 | $2.27 | $-25.51 | $9,616.18 | ▼ -25.51 after sell → book $10,884.43; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `ORBS` | 1425 | $0.89 | $17.20 | $+3.26 | $10,867.23 | ▲ +3.26 after sell → book $10,867.23; vs 09:30 mark -17.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,867.23 | ▲ 09:30 equity $10,867.23 vs yday $10,867.23 (-0.00) | 09:30 open · cash $10,867.23 · no holdings · equity $10,867.23 vs prior close $10,867.23 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 838 | $1.62 | $10.81 | — | $9,498.86 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1358.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 679 | $2.00 | $8.76 | — | $8,132.10 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1358.40 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 367 | $3.70 | $4.73 | — | $6,769.47 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1358.40 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 260 | $5.22 | $3.35 | — | $5,408.91 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1358.40 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 200 | $6.79 | $2.59 | — | $4,048.32 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1358.40 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALIT` | 91 | $14.86 | $2.26 | — | $2,693.80 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=+3.0; leftover $1358.40 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 judge🟡 ab🟢 peer🟡 vol🟡 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 212 | $6.38 | $2.73 | — | $1,338.50 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1358.40 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SAFX` | 3551 | $0.37 | $23.79 | — | $0.84 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=-26.5; leftover $1358.40 | join🔴 sector🟡 gen🟡 news🟡 digest🟡 ab🔴 vol🔴 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.84 | ▲ 09:30 equity $11,004.61 vs yday $11,004.61 (+0.00) | 09:30 open · cash $0.84 (unchanged overnight, no fees) · equity $11,004.61 vs prior close $11,004.61 (+0.00) because holdings re-marked: BMEA×838 yday $1.61 → 09:30 $1.61 +0.00; NPWR×679 yday $2.02 → 09:30 $2.02 +0.00; PUSA×367 yday $3.91 → 09:30 $3.91 +0.00; ALVO×260 yday $5.25 → 09:30 $5.25 +0.00; CAPR×200 yday $7.19 → 09:30 $7.19 +0.00; ALIT×91 yday $14.87 → 09:30 $14.87 +0.00; ZURA×212 yday $6.50 → 09:30 $6.50 +0.00; SAFX×3551 yday $0.37 → 09:30 $0.37 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $0.84 | ▲ 09:30 equity $11,033.65 vs yday $10,808.19 (+225.46) | 09:30 open · cash $0.84 (unchanged overnight, no fees) · equity $11,033.65 vs prior close $10,808.19 (+225.46) because holdings re-marked: BMEA×838 yday $1.61 → 09:30 $1.75 +117.32; NPWR×679 yday $2.02 → 09:30 $1.93 -61.11; PUSA×367 yday $3.91 → 09:30 $3.84 -25.69; ALVO×260 yday $5.25 → 09:30 $4.98 -70.20; CAPR×200 yday $7.19 → 09:30 $8.29 +220.00; ALIT×91 yday $14.87 → 09:30 $14.85 -1.82; ZURA×212 yday $6.50 → 09:30 $6.13 -78.44; SAFX×3551 yday $0.37 → 09:30 $0.35 -71.02 | — |
| 2026-08-27 09:30 ET | **SELL** | `BMEA` | 838 | $1.75 | $10.96 | $+87.17 | $1,456.38 | ▲ +87.17 after sell → book $11,022.69; vs 09:30 mark -10.96 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 679 | $1.93 | $8.88 | $-65.17 | $2,757.97 | ▼ -65.17 after sell → book $11,013.81; vs 09:30 mark -8.88 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `PUSA` | 367 | $3.84 | $4.81 | $+41.84 | $4,162.44 | ▲ +41.84 after sell → book $11,009.00; vs 09:30 mark -4.81 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 260 | $4.98 | $3.41 | $-69.16 | $5,453.84 | ▼ -69.16 after sell → book $11,005.60; vs 09:30 mark -3.40 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CAPR` | 200 | $8.29 | $2.64 | $+294.77 | $7,109.20 | ▲ +294.77 after sell → book $11,002.96; vs 09:30 mark -2.64 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALIT` | 91 | $14.85 | $2.29 | $-5.46 | $8,458.26 | ▼ -5.46 after sell → book $11,000.67; vs 09:30 mark -2.29 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 212 | $6.13 | $2.78 | $-58.52 | $9,755.04 | ▼ -58.52 after sell → book $10,997.89; vs 09:30 mark -2.78 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `SAFX` | 3551 | $0.35 | $23.68 | $-118.49 | $10,974.21 | ▼ -118.49 after sell → book $10,974.21; vs 09:30 mark -23.68 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,974.21 | ▲ 09:30 equity $10,974.21 vs yday $10,974.21 (-0.00) | 09:30 open · cash $10,974.21 · no holdings · equity $10,974.21 vs prior close $10,974.21 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $9,669.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1371.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 80 | $16.95 | $2.23 | — | $8,311.66 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1371.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 74 | $18.50 | $2.21 | — | $6,940.45 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1371.78 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `CAPR` | 149 | $9.19 | $2.44 | — | $5,568.70 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1371.78 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `LVWR` | 994 | $1.38 | $12.82 | — | $4,184.16 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+0.0; leftover $1371.78 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 peer🟢 heat🔴 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 40 | $33.78 | $2.11 | — | $2,830.85 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1371.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $1,484.23 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1371.78 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GRRR` | 86 | $15.94 | $2.25 | — | $111.15 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-1.9; leftover $1371.78 | join🟢 sector🟢 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $111.15 | ▼ 09:30 equity $10,465.46 vs yday $10,864.31 (-398.85) | 09:30 open · cash $111.15 (unchanged overnight, no fees) · equity $10,465.46 vs prior close $10,864.31 (-398.85) because holdings re-marked: ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×80 yday $16.12 → 09:30 $15.44 -54.40; BZ×74 yday $18.00 → 09:30 $17.89 -8.14; CAPR×149 yday $10.06 → 09:30 $9.44 -92.38; LVWR×994 yday $1.36 → 09:30 $1.37 +9.94; SEDG×40 yday $33.51 → 09:30 $31.50 -80.40; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; GRRR×86 yday $15.66 → 09:30 $14.32 -115.24 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 9 | $148.67 | $2.04 | $+31.68 | $1,447.14 | ▲ +31.68 after sell → book $10,463.42; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 80 | $15.44 | $2.25 | $-125.28 | $2,680.08 | ▼ -125.28 after sell → book $10,461.16; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 74 | $17.89 | $2.23 | $-49.59 | $4,001.71 | ▼ -49.59 after sell → book $10,458.93; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `CAPR` | 149 | $9.44 | $2.47 | $+32.34 | $5,405.80 | ▲ +32.34 after sell → book $10,456.46; vs 09:30 mark -2.47 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `LVWR` | 994 | $1.37 | $13.00 | $-35.76 | $6,754.58 | ▼ -35.76 after sell → book $10,443.46; vs 09:30 mark -13.00 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SEDG` | 40 | $31.50 | $2.13 | $-95.44 | $8,012.45 | ▼ -95.44 after sell → book $10,441.33; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `SMTC` | 9 | $133.04 | $2.04 | $-151.29 | $9,207.77 | ▼ -151.29 after sell → book $10,439.29; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `GRRR` | 86 | $14.32 | $2.27 | $-143.84 | $10,437.02 | ▼ -143.84 after sell → book $10,437.02; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,437.02 | ▲ 09:30 equity $10,437.02 vs yday $10,437.02 (-0.00) | 09:30 open · cash $10,437.02 · no holdings · equity $10,437.02 vs prior close $10,437.02 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,437.02 | ▲ 09:30 equity $10,437.02 vs yday $10,437.02 (-0.00) | 09:30 open · cash $10,437.02 · no holdings · equity $10,437.02 vs prior close $10,437.02 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,437.02 | ▲ 09:30 equity $10,437.02 vs yday $10,437.02 (-0.00) | 09:30 open · cash $10,437.02 · no holdings · equity $10,437.02 vs prior close $10,437.02 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 1069 | $1.22 | $13.79 | — | $9,119.05 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1304.63 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 70 | $18.40 | $2.20 | — | $7,828.85 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $1304.63 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 83 | $15.70 | $2.24 | — | $6,523.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1304.63 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 57 | $22.78 | $2.16 | — | $5,222.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1304.63 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 350 | $3.72 | $4.51 | — | $3,916.37 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1304.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `SLN` | 88 | $14.70 | $2.25 | — | $2,620.52 | — | baseline list, no extra gate; list probable,yday_gainer; 🔵; ⚪; ret5=-3.3; leftover $1304.63 | join🟢 sector🟡 gen🟡 news🔴 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `EIX` | 22 | $56.78 | $2.06 | — | $1,369.30 | — | baseline list, no extra gate; list probable,yday_gainer; ret5=+0.3; leftover $1304.63 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 603 | $2.16 | $7.78 | — | $59.04 | — | baseline list, no extra gate; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1304.63 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $59.04 | ▲ 09:30 equity $11,030.35 vs yday $10,894.60 (+135.75) | 09:30 open · cash $59.04 (unchanged overnight, no fees) · equity $11,030.35 vs prior close $10,894.60 (+135.75) because holdings re-marked: GPRO×1069 yday $1.69 → 09:30 $1.78 +96.21; FRVO×70 yday $17.98 → 09:30 $18.27 +20.30; CRK×83 yday $15.54 → 09:30 $15.45 -7.47; MMED×57 yday $23.76 → 09:30 $23.88 +6.84; CTMX×350 yday $3.72 → 09:30 $3.73 +3.50; SLN×88 yday $14.79 → 09:30 $14.85 +5.28; EIX×22 yday $55.19 → 09:30 $55.42 +5.06; CRDL×603 yday $2.17 → 09:30 $2.18 +6.03 | — |
| 2026-09-04 09:30 ET | **SELL** | `FRVO` | 70 | $18.27 | $2.22 | $-13.52 | $1,335.72 | ▼ -13.52 after sell → book $11,028.13; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 83 | $15.45 | $2.26 | $-25.25 | $2,615.81 | ▼ -25.25 after sell → book $11,025.87; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 57 | $23.88 | $2.18 | $+58.36 | $3,974.79 | ▲ +58.36 after sell → book $11,023.69; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CTMX` | 350 | $3.73 | $4.58 | $-5.60 | $5,275.70 | ▼ -5.60 after sell → book $11,019.10; vs 09:30 mark -4.59 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `SLN` | 88 | $14.85 | $2.28 | $+8.67 | $6,580.23 | ▲ +8.67 after sell → book $11,016.83; vs 09:30 mark -2.27 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `EIX` | 22 | $55.42 | $2.08 | $-34.05 | $7,797.39 | ▼ -34.05 after sell → book $11,014.75; vs 09:30 mark -2.08 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRDL` | 603 | $2.18 | $7.89 | $-3.61 | $9,104.04 | ▼ -3.61 after sell → book $11,006.86; vs 09:30 mark -7.89 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 666 | $1.95 | $8.59 | — | $7,796.75 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1300.58 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 364 | $3.57 | $4.70 | — | $6,492.57 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1300.58 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SLBT` | 423 | $3.07 | $5.46 | — | $5,188.51 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1300.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,213.89 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1300.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 44 | $29.15 | $2.12 | — | $2,929.17 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1300.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 127 | $10.22 | $2.37 | — | $1,628.86 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1300.58 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SION` | 177 | $7.31 | $2.52 | — | $332.47 | — | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1300.58 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🔴 heat🟢 vol🟡 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
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
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `SAFX` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `INFQ` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `ALOY` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `OI` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `BMEA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `PUSA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALIT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `SAFX` | no_price | no 09:30 open — carry |
| 2026-08-26 | `RZLT` | no_price | no 09:30 open |
| 2026-08-26 | `KURA` | no_price | no 09:30 open |
| 2026-08-26 | `AVBP` | no_price | no 09:30 open |
| 2026-08-26 | `FLNC` | no_price | no 09:30 open |
| 2026-08-26 | `ABX` | no_price | no 09:30 open |
| 2026-08-26 | `AVEX` | no_price | no 09:30 open |
| 2026-08-26 | `ITG` | no_price | no 09:30 open |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RPD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `ACDC` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `BRUN` | hard_red | hard-red S=-5.85 sit; no new buys |
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

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 1069 | 2026-09-03 @ $1.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1304.63 |
| `BAK` | 666 | 2026-09-04 @ $1.95 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1300.58 |
| `EOSE` | 364 | 2026-09-04 @ $3.57 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1300.58 |
| `SLBT` | 423 | 2026-09-04 @ $3.07 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-0.4; leftover $1300.58 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1300.58 |
| `MLYS` | 44 | 2026-09-04 @ $29.15 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1300.58 |
| `CCOI` | 127 | 2026-09-04 @ $10.22 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1300.58 |
| `SION` | 177 | 2026-09-04 @ $7.31 | baseline list, no extra gate; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.1; leftover $1300.58 |
