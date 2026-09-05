# Factor mine action — `union_vol_green_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-10.93%** ($8,907) · signal-only (no cash/fees) was -13.47%. Starts YES **5/17**. Fills 79 · skips 123 · realized $-1184.77.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $45.62.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,759.50 | -42.47 | — | — | $3.57 | $9,785.73 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,759.50 vs prior close $9,801.97 (-42.47) because holdings re-marked: BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; QMLS×170 yday $7.32 → 09:30 $7.24 -13.60 |
| 2026-08-18 | -6.20 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,552.99 | -232.74 | — | — | $3.57 | $9,361.35 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,552.99 vs prior close $9,785.73 (-232.74) because holdings re-marked: BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28; QMLS×170 yday $7.14 → 09:30 $6.85 -49.30 |
| 2026-08-19 | -7.20 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,353.77 | -7.58 | — | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $9,319.69 | $9,319.69 | — | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,353.77 vs prior close $9,361.35 (-7.58) because holdings re-marked: BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56; QMLS×170 yday $6.74 → 09:30 $6.74 +0.00 |
| 2026-08-20 | +1.12 | $9,319.69 | — | $9,319.69 | -0.00 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $32.96 | $9,448.07 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236 | 09:30 open · cash $9,319.69 · no holdings · equity $9,319.69 vs prior close $9,319.69 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $32.96 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236 | $9,775.84 | +327.77 | CYPH, BTBT, GORO | — | $22.45 | $9,760.25 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, GORO×1 | 09:30 open · cash $32.96 (unchanged overnight, no fees) · equity $9,775.84 vs prior close $9,448.07 (+327.77) because holdings re-marked: AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×665 yday $1.75 → 09:30 $1.79 +26.60; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 |
| 2026-08-24 | -5.17 | $22.45 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, GORO×1 | $9,862.02 | +101.77 | — | — | $22.45 | $9,734.26 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, GORO×1 | 09:30 open · cash $22.45 (unchanged overnight, no fees) · equity $9,862.02 vs prior close $9,760.25 (+101.77) because holdings re-marked: AG×56 yday $21.09 → 09:30 $21.47 +21.28; CDE×56 yday $20.97 → 09:30 $21.26 +16.24; HDSN×201 yday $5.63 → 09:30 $5.69 +12.06; IAG×59 yday $21.14 → 09:30 $21.44 +17.70; KGC×39 yday $32.76 → 09:30 $33.21 +17.55; NFGC×665 yday $1.84 → 09:30 $1.86 +13.30; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×236 yday $5.21 → 09:30 $5.18 -7.08; CYPH×3 yday $1.42 → 09:30 $1.83 +1.23; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; GORO×1 yday $3.19 → 09:30 $3.20 +0.01 |
| 2026-08-25 | +1.80 | $22.45 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×665, WPM×8, ABUS×236, CYPH×3, BTBT×2, GORO×1 | $9,819.51 | +85.25 | NPWR, ALVO, ZURA, DEFT, ASST, BMNR | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $7.93 | $9,665.45 | CYPH×3, BTBT×2, GORO×1, NPWR×815, ALVO×312, ZURA×255, DEFT×2547, ASST×78, BMNR×64 | 09:30 open · cash $22.45 (unchanged overnight, no fees) · equity $9,819.51 vs prior close $9,734.26 (+85.25) because holdings re-marked: AG×56 yday $20.57 → 09:30 $20.73 +8.96; CDE×56 yday $20.49 → 09:30 $20.85 +20.16; HDSN×201 yday $5.57 → 09:30 $5.53 -8.04; IAG×59 yday $21.36 → 09:30 $21.63 +15.93; KGC×39 yday $32.47 → 09:30 $32.76 +11.31; NFGC×665 yday $1.90 → 09:30 $1.91 +6.65; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×236 yday $5.20 → 09:30 $5.26 +14.16; CYPH×3 yday $1.64 → 09:30 $1.70 +0.18; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02; GORO×1 yday $3.57 → 09:30 $3.53 -0.04 |
| 2026-08-26 | +2.02 | $7.93 | CYPH×3, BTBT×2, GORO×1, NPWR×815, ALVO×312, ZURA×255, DEFT×2547, ASST×78, BMNR×64 | $9,665.45 | -0.00 | — | — | $7.93 | $9,748.20 | CYPH×3, BTBT×2, GORO×1, NPWR×815, ALVO×312, ZURA×255, DEFT×2547, ASST×78, BMNR×64 | 09:30 open · cash $7.93 (unchanged overnight, no fees) · equity $9,665.45 vs prior close $9,665.45 (-0.00) because holdings re-marked: CYPH×3 yday $1.64 → 09:30 $1.64 +0.00; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; GORO×1 yday $3.56 → 09:30 $3.56 +0.00; NPWR×815 yday $2.02 → 09:30 $2.02 +0.00; ALVO×312 yday $5.25 → 09:30 $5.25 +0.00; ZURA×255 yday $6.50 → 09:30 $6.50 +0.00; DEFT×2547 yday $0.62 → 09:30 $0.62 +0.00; ASST×78 yday $20.20 → 09:30 $20.20 +0.00; BMNR×64 yday $24.21 → 09:30 $24.21 +0.00 |
| 2026-08-27 | — | $7.93 | CYPH×3, BTBT×2, GORO×1, NPWR×815, ALVO×312, ZURA×255, DEFT×2547, ASST×78, BMNR×64 | $9,405.14 | -343.06 | — | CYPH, BTBT, GORO | $19.36 | $9,327.85 | NPWR×815, ALVO×312, ZURA×255, DEFT×2547, ASST×78, BMNR×64 | 09:30 open · cash $7.93 (unchanged overnight, no fees) · equity $9,405.14 vs prior close $9,748.20 (-343.06) because holdings re-marked: CYPH×3 yday $1.64 → 09:30 $1.60 -0.12; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; GORO×1 yday $3.56 → 09:30 $3.77 +0.21; NPWR×815 yday $2.02 → 09:30 $1.93 -73.35; ALVO×312 yday $5.25 → 09:30 $4.98 -84.24; ZURA×255 yday $6.50 → 09:30 $6.13 -94.35; DEFT×2547 yday $0.62 → 09:30 $0.60 -50.94; ASST×78 yday $20.20 → 09:30 $20.72 +40.56; BMNR×64 yday $24.21 → 09:30 $24.24 +1.92 |
| 2026-08-28 | +0.75 | $19.36 | NPWR×815, ALVO×312, ZURA×255, DEFT×2547, ASST×78, BMNR×64 | $9,506.01 | +178.16 | ANF, BHVN, BZ, URBN, ERAS, ZYME, GENB, TIGR | NPWR, ALVO, ZURA, DEFT, ASST, BMNR | $81.23 | $9,121.89 | ANF×8, BHVN×69, BZ×63, URBN×14, ERAS×61, ZYME×40, GENB×69, TIGR×215 | 09:30 open · cash $19.36 (unchanged overnight, no fees) · equity $9,506.01 vs prior close $9,327.85 (+178.16) because holdings re-marked: NPWR×815 yday $1.81 → 09:30 $1.83 +16.30; ALVO×312 yday $4.91 → 09:30 $4.88 -9.36; ZURA×255 yday $5.99 → 09:30 $6.02 +7.65; DEFT×2547 yday $0.59 → 09:30 $0.60 +25.47; ASST×78 yday $21.50 → 09:30 $22.45 +74.10; BMNR×64 yday $24.91 → 09:30 $25.91 +64.00 |
| 2026-08-31 | -5.85 | $81.23 | ANF×8, BHVN×69, BZ×63, URBN×14, ERAS×61, ZYME×40, GENB×69, TIGR×215 | $8,945.15 | -176.74 | — | — | $81.23 | $8,960.03 | ANF×8, BHVN×69, BZ×63, URBN×14, ERAS×61, ZYME×40, GENB×69, TIGR×215 | 09:30 open · cash $81.23 (unchanged overnight, no fees) · equity $8,945.15 vs prior close $9,121.89 (-176.74) because holdings re-marked: ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×69 yday $16.12 → 09:30 $15.44 -46.92; BZ×63 yday $18.00 → 09:30 $17.89 -6.93; URBN×14 yday $78.79 → 09:30 $81.09 +32.20; ERAS×61 yday $19.49 → 09:30 $17.90 -96.99; ZYME×40 yday $29.01 → 09:30 $28.27 -29.60; GENB×69 yday $15.77 → 09:30 $15.33 -30.36; TIGR×215 yday $5.06 → 09:30 $4.96 -21.50 |
| 2026-09-01 | -6.30 | $81.23 | ANF×8, BHVN×69, BZ×63, URBN×14, ERAS×61, ZYME×40, GENB×69, TIGR×215 | $8,931.30 | -28.73 | — | — | $81.23 | $8,886.25 | ANF×8, BHVN×69, BZ×63, URBN×14, ERAS×61, ZYME×40, GENB×69, TIGR×215 | 09:30 open · cash $81.23 (unchanged overnight, no fees) · equity $8,931.30 vs prior close $8,960.03 (-28.73) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BHVN×69 yday $15.40 → 09:30 $15.45 +3.45; BZ×63 yday $17.90 → 09:30 $17.37 -33.39; URBN×14 yday $81.09 → 09:30 $80.69 -5.60; ERAS×61 yday $17.90 → 09:30 $18.00 +6.10; ZYME×40 yday $28.27 → 09:30 $29.32 +42.00; GENB×69 yday $15.35 → 09:30 $15.51 +11.04; TIGR×215 yday $5.01 → 09:30 $5.02 +2.15 |
| 2026-09-02 | -3.83 | $81.23 | ANF×8, BHVN×69, BZ×63, URBN×14, ERAS×61, ZYME×40, GENB×69, TIGR×215 | $8,833.10 | -53.15 | — | ANF, BHVN, BZ, URBN, ERAS, ZYME, GENB, TIGR | $8,815.23 | $8,815.23 | — | 09:30 open · cash $81.23 (unchanged overnight, no fees) · equity $8,833.10 vs prior close $8,886.25 (-53.15) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BHVN×69 yday $15.45 → 09:30 $15.39 -4.14; BZ×63 yday $17.17 → 09:30 $17.29 +7.56; URBN×14 yday $80.69 → 09:30 $79.12 -21.98; ERAS×61 yday $17.70 → 09:30 $17.58 -7.32; ZYME×40 yday $29.33 → 09:30 $29.32 -0.40; GENB×69 yday $15.30 → 09:30 $15.12 -12.42; TIGR×215 yday $5.00 → 09:30 $4.97 -6.45 |
| 2026-09-03 | -0.90 | $8,815.23 | — | $8,815.23 | +0.00 | RVTY, GPRO, CRK, MMED, DEFT, MRNA, ARCT, NVAX | — | $126.63 | $9,261.85 | RVTY×8, GPRO×903, CRK×70, MMED×48, DEFT×1644, MRNA×7, ARCT×66, NVAX×107 | 09:30 open · cash $8,815.23 · no holdings · equity $8,815.23 vs prior close $8,815.23 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $126.63 | RVTY×8, GPRO×903, CRK×70, MMED×48, DEFT×1644, MRNA×7, ARCT×66, NVAX×107 | $9,332.25 | +70.40 | BAK, EOSE, OABI, ALEC, FMC | — | $45.62 | $8,907.25 | RVTY×8, GPRO×903, CRK×70, MMED×48, DEFT×1644, MRNA×7, ARCT×66, NVAX×107, BAK×9, EOSE×5, OABI×3, ALEC×6, FMC×1 | 09:30 open · cash $126.63 (unchanged overnight, no fees) · equity $9,332.25 vs prior close $9,261.85 (+70.40) because holdings re-marked: RVTY×8 yday $130.94 → 09:30 $132.45 +12.08; GPRO×903 yday $1.69 → 09:30 $1.78 +81.27; CRK×70 yday $15.54 → 09:30 $15.45 -6.30; MMED×48 yday $23.76 → 09:30 $23.88 +5.76; DEFT×1644 yday $0.65 → 09:30 $0.65 +0.00; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×66 yday $16.74 → 09:30 $16.77 +1.98; NVAX×107 yday $10.32 → 09:30 $10.41 +9.63 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,last_green=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,last_green=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $2,499.51 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $1,245.37 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `QMLS` | 170 | $7.29 | $2.50 | — | $3.57 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,759.50 vs yday $9,801.97 (-42.47) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,759.50 vs prior close $9,801.97 (-42.47) because holdings re-marked: BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; QMLS×170 yday $7.32 → 09:30 $7.24 -13.60 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,552.99 vs yday $9,785.73 (-232.74) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,552.99 vs prior close $9,785.73 (-232.74) because holdings re-marked: BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28; QMLS×170 yday $7.14 → 09:30 $6.85 -49.30 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $3.57 | ▼ 09:30 equity $9,353.77 vs yday $9,361.35 (-7.58) | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,353.77 vs prior close $9,361.35 (-7.58) because holdings re-marked: BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56; QMLS×170 yday $6.74 → 09:30 $6.74 +0.00 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $1,175.53 | ▼ -88.28 after sell → book $9,342.87; vs 09:30 mark -10.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 84 | $13.03 | $2.27 | $-153.19 | $2,267.79 | ▼ -153.19 after sell → book $9,340.61; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $3,653.09 | ▲ +131.66 after sell → book $9,336.81; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $4,806.30 | ▼ -100.46 after sell → book $9,332.89; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $5,977.81 | ▼ -68.20 after sell → book $9,330.65; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $6,994.66 | ▼ -230.92 after sell → book $9,328.30; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $8,176.43 | ▼ -72.38 after sell → book $9,322.23; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `QMLS` | 170 | $6.74 | $2.54 | $-98.54 | $9,319.69 | ▼ -98.54 after sell → book $9,319.69; vs 09:30 mark -2.54 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,319.69 | ▲ 09:30 equity $9,319.69 vs yday $9,319.69 (-0.00) | 09:30 open · cash $9,319.69 · no holdings · equity $9,319.69 vs prior close $9,319.69 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,166.73 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $7,008.17 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,845.80 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,685.47 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,527.79 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 665 | $1.75 | $8.58 | — | $2,355.46 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,197.13 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $32.96 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1164.96 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $32.96 | ▲ 09:30 equity $9,775.84 vs yday $9,448.07 (+327.77) | 09:30 open · cash $32.96 (unchanged overnight, no fees) · equity $9,775.84 vs prior close $9,448.07 (+327.77) because holdings re-marked: AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×665 yday $1.75 → 09:30 $1.79 +26.60; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 | — |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 3 | $1.32 | $0.05 | — | $28.95 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $4.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 2 | $1.66 | $0.04 | — | $25.60 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $4.12 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 1 | $3.11 | $0.03 | — | $22.45 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+7.1; leftover $4.12 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.45 | ▲ 09:30 equity $9,862.02 vs yday $9,760.25 (+101.77) | 09:30 open · cash $22.45 (unchanged overnight, no fees) · equity $9,862.02 vs prior close $9,760.25 (+101.77) because holdings re-marked: AG×56 yday $21.09 → 09:30 $21.47 +21.28; CDE×56 yday $20.97 → 09:30 $21.26 +16.24; HDSN×201 yday $5.63 → 09:30 $5.69 +12.06; IAG×59 yday $21.14 → 09:30 $21.44 +17.70; KGC×39 yday $32.76 → 09:30 $33.21 +17.55; NFGC×665 yday $1.84 → 09:30 $1.86 +13.30; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; ABUS×236 yday $5.21 → 09:30 $5.18 -7.08; CYPH×3 yday $1.42 → 09:30 $1.83 +1.23; BTBT×2 yday $1.53 → 09:30 $1.55 +0.04; GORO×1 yday $3.19 → 09:30 $3.20 +0.01 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $22.45 | ▲ 09:30 equity $9,819.51 vs yday $9,734.26 (+85.25) | 09:30 open · cash $22.45 (unchanged overnight, no fees) · equity $9,819.51 vs prior close $9,734.26 (+85.25) because holdings re-marked: AG×56 yday $20.57 → 09:30 $20.73 +8.96; CDE×56 yday $20.49 → 09:30 $20.85 +20.16; HDSN×201 yday $5.57 → 09:30 $5.53 -8.04; IAG×59 yday $21.36 → 09:30 $21.63 +15.93; KGC×39 yday $32.47 → 09:30 $32.76 +11.31; NFGC×665 yday $1.90 → 09:30 $1.91 +6.65; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; ABUS×236 yday $5.20 → 09:30 $5.26 +14.16; CYPH×3 yday $1.64 → 09:30 $1.70 +0.18; BTBT×2 yday $1.56 → 09:30 $1.55 -0.02; GORO×1 yday $3.57 → 09:30 $3.53 -0.04 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 56 | $20.73 | $2.18 | $+5.74 | $1,181.15 | ▲ +5.74 after sell → book $9,817.33; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.85 | $2.18 | $+6.86 | $2,346.58 | ▲ +6.86 after sell → book $9,815.16; vs 09:30 mark -2.17 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 201 | $5.53 | $2.64 | $-53.48 | $3,455.46 | ▼ -53.48 after sell → book $9,812.51; vs 09:30 mark -2.65 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.63 | $2.19 | $+113.65 | $4,729.45 | ▲ +113.65 after sell → book $9,810.33; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.76 | $2.13 | $+117.84 | $6,004.96 | ▲ +117.84 after sell → book $9,808.20; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 665 | $1.91 | $8.70 | $+89.12 | $7,266.41 | ▲ +89.12 after sell → book $9,799.50; vs 09:30 mark -8.70 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $8,544.38 | ▲ +119.63 after sell → book $9,797.47; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **SELL** | `ABUS` | 236 | $5.26 | $3.09 | $+74.10 | $9,782.64 | ▲ +74.10 after sell → book $9,794.37; vs 09:30 mark -3.10 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 815 | $2.00 | $10.51 | — | $8,142.13 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1630.44 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 312 | $5.22 | $4.02 | — | $6,509.46 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1630.44 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 255 | $6.38 | $3.29 | — | $4,879.27 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1630.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2547 | $0.64 | $23.94 | — | $3,225.25 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1630.44 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 78 | $20.90 | $2.22 | — | $1,592.83 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+47.9; leftover $1630.44 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 64 | $24.73 | $2.18 | — | $7.93 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=+26.3; leftover $1630.44 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.93 | ▲ 09:30 equity $9,665.45 vs yday $9,665.45 (-0.00) | 09:30 open · cash $7.93 (unchanged overnight, no fees) · equity $9,665.45 vs prior close $9,665.45 (-0.00) because holdings re-marked: CYPH×3 yday $1.64 → 09:30 $1.64 +0.00; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; GORO×1 yday $3.56 → 09:30 $3.56 +0.00; NPWR×815 yday $2.02 → 09:30 $2.02 +0.00; ALVO×312 yday $5.25 → 09:30 $5.25 +0.00; ZURA×255 yday $6.50 → 09:30 $6.50 +0.00; DEFT×2547 yday $0.62 → 09:30 $0.62 +0.00; ASST×78 yday $20.20 → 09:30 $20.20 +0.00; BMNR×64 yday $24.21 → 09:30 $24.21 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $7.93 | ▼ 09:30 equity $9,405.14 vs yday $9,748.20 (-343.06) | 09:30 open · cash $7.93 (unchanged overnight, no fees) · equity $9,405.14 vs prior close $9,748.20 (-343.06) because holdings re-marked: CYPH×3 yday $1.64 → 09:30 $1.60 -0.12; BTBT×2 yday $1.53 → 09:30 $1.53 +0.00; GORO×1 yday $3.56 → 09:30 $3.77 +0.21; NPWR×815 yday $2.02 → 09:30 $1.93 -73.35; ALVO×312 yday $5.25 → 09:30 $4.98 -84.24; ZURA×255 yday $6.50 → 09:30 $6.13 -94.35; DEFT×2547 yday $0.62 → 09:30 $0.60 -50.94; ASST×78 yday $20.20 → 09:30 $20.72 +40.56; BMNR×64 yday $24.21 → 09:30 $24.24 +1.92 | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 3 | $1.60 | $0.08 | $+0.71 | $12.65 | ▲ +0.71 after sell → book $9,405.06; vs 09:30 mark -0.08 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `BTBT` | 2 | $1.53 | $0.06 | $-0.36 | $15.65 | ▼ -0.36 after sell → book $9,405.00; vs 09:30 mark -0.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 1 | $3.77 | $0.06 | $+0.57 | $19.36 | ▲ +0.57 after sell → book $9,404.94; vs 09:30 mark -0.06 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $19.36 | ▲ 09:30 equity $9,506.01 vs yday $9,327.85 (+178.16) | 09:30 open · cash $19.36 (unchanged overnight, no fees) · equity $9,506.01 vs prior close $9,327.85 (+178.16) because holdings re-marked: NPWR×815 yday $1.81 → 09:30 $1.83 +16.30; ALVO×312 yday $4.91 → 09:30 $4.88 -9.36; ZURA×255 yday $5.99 → 09:30 $6.02 +7.65; DEFT×2547 yday $0.59 → 09:30 $0.60 +25.47; ASST×78 yday $21.50 → 09:30 $22.45 +74.10; BMNR×64 yday $24.91 → 09:30 $25.91 +64.00 | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 815 | $1.83 | $10.66 | $-159.72 | $1,500.15 | ▼ -159.72 after sell → book $9,495.35; vs 09:30 mark -10.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 312 | $4.88 | $4.09 | $-114.19 | $3,018.62 | ▼ -114.19 after sell → book $9,491.26; vs 09:30 mark -4.09 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 255 | $6.02 | $3.34 | $-98.43 | $4,550.38 | ▼ -98.43 after sell → book $9,487.92; vs 09:30 mark -3.34 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `DEFT` | 2547 | $0.60 | $23.36 | $-149.18 | $6,055.22 | ▼ -149.18 after sell → book $9,464.56; vs 09:30 mark -23.36 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 heat🟡 vol🔴 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ASST` | 78 | $22.45 | $2.25 | $+116.43 | $7,804.07 | ▲ +116.43 after sell → book $9,462.31; vs 09:30 mark -2.25 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `BMNR` | 64 | $25.91 | $2.21 | $+71.13 | $9,460.10 | ▲ +71.13 after sell → book $9,460.10; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $8,300.49 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1182.51 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 69 | $16.95 | $2.20 | — | $7,128.74 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1182.51 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 63 | $18.50 | $2.18 | — | $5,961.06 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1182.51 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 14 | $82.70 | $2.03 | — | $4,801.23 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1182.51 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 61 | $19.30 | $2.17 | — | $3,621.76 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=-4.1; leftover $1182.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 40 | $29.33 | $2.11 | — | $2,446.45 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1182.51 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 69 | $17.10 | $2.20 | — | $1,264.35 | — | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=+3.1; leftover $1182.51 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 215 | $5.49 | $2.77 | — | $81.23 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+15.9; leftover $1182.51 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.23 | ▼ 09:30 equity $8,945.15 vs yday $9,121.89 (-176.74) | 09:30 open · cash $81.23 (unchanged overnight, no fees) · equity $8,945.15 vs prior close $9,121.89 (-176.74) because holdings re-marked: ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×69 yday $16.12 → 09:30 $15.44 -46.92; BZ×63 yday $18.00 → 09:30 $17.89 -6.93; URBN×14 yday $78.79 → 09:30 $81.09 +32.20; ERAS×61 yday $19.49 → 09:30 $17.90 -96.99; ZYME×40 yday $29.01 → 09:30 $28.27 -29.60; GENB×69 yday $15.77 → 09:30 $15.33 -30.36; TIGR×215 yday $5.06 → 09:30 $4.96 -21.50 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.23 | ▼ 09:30 equity $8,931.30 vs yday $8,960.03 (-28.73) | 09:30 open · cash $81.23 (unchanged overnight, no fees) · equity $8,931.30 vs prior close $8,960.03 (-28.73) because holdings re-marked: ANF×8 yday $149.28 → 09:30 $142.47 -54.48; BHVN×69 yday $15.40 → 09:30 $15.45 +3.45; BZ×63 yday $17.90 → 09:30 $17.37 -33.39; URBN×14 yday $81.09 → 09:30 $80.69 -5.60; ERAS×61 yday $17.90 → 09:30 $18.00 +6.10; ZYME×40 yday $28.27 → 09:30 $29.32 +42.00; GENB×69 yday $15.35 → 09:30 $15.51 +11.04; TIGR×215 yday $5.01 → 09:30 $5.02 +2.15 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $81.23 | ▼ 09:30 equity $8,833.10 vs yday $8,886.25 (-53.15) | 09:30 open · cash $81.23 (unchanged overnight, no fees) · equity $8,833.10 vs prior close $8,886.25 (-53.15) because holdings re-marked: ANF×8 yday $143.00 → 09:30 $142.00 -8.00; BHVN×69 yday $15.45 → 09:30 $15.39 -4.14; BZ×63 yday $17.17 → 09:30 $17.29 +7.56; URBN×14 yday $80.69 → 09:30 $79.12 -21.98; ERAS×61 yday $17.70 → 09:30 $17.58 -7.32; ZYME×40 yday $29.33 → 09:30 $29.32 -0.40; GENB×69 yday $15.30 → 09:30 $15.12 -12.42; TIGR×215 yday $5.00 → 09:30 $4.97 -6.45 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 8 | $142.00 | $2.03 | $-25.65 | $1,215.19 | ▼ -25.65 after sell → book $8,831.06; vs 09:30 mark -2.04 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BHVN` | 69 | $15.39 | $2.22 | $-112.06 | $2,274.89 | ▼ -112.06 after sell → book $8,828.85; vs 09:30 mark -2.21 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `BZ` | 63 | $17.29 | $2.20 | $-80.61 | $3,361.96 | ▼ -80.61 after sell → book $8,826.65; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 14 | $79.12 | $2.05 | $-54.20 | $4,467.58 | ▼ -54.20 after sell → book $8,824.59; vs 09:30 mark -2.06 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ERAS` | 61 | $17.58 | $2.19 | $-109.29 | $5,537.77 | ▼ -109.29 after sell → book $8,822.40; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `ZYME` | 40 | $29.32 | $2.13 | $-4.64 | $6,708.44 | ▼ -4.64 after sell → book $8,820.27; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **SELL** | `GENB` | 69 | $15.12 | $2.22 | $-141.04 | $7,749.50 | ▼ -141.04 after sell → book $8,818.05; vs 09:30 mark -2.22 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `TIGR` | 215 | $4.97 | $2.82 | $-117.39 | $8,815.23 | ▼ -117.39 after sell → book $8,815.23; vs 09:30 mark -2.82 | dropped from list after 3 sess (min 3) | join🟢 sector🔴 gen🔴 news🟡 digest🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $8,815.23 | ▲ 09:30 equity $8,815.23 vs yday $8,815.23 (+0.00) | 09:30 open · cash $8,815.23 · no holdings · equity $8,815.23 vs prior close $8,815.23 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 8 | $125.94 | $2.01 | — | $7,805.70 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1101.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 903 | $1.22 | $11.65 | — | $6,692.39 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1101.90 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 70 | $15.70 | $2.20 | — | $5,591.19 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1101.90 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 48 | $22.78 | $2.13 | — | $4,495.62 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1101.90 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1644 | $0.67 | $15.95 | — | $3,378.19 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1101.90 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $2,316.38 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1101.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 66 | $16.46 | $2.19 | — | $1,227.83 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1101.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 107 | $10.27 | $2.31 | — | $126.63 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1101.90 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $126.63 | ▲ 09:30 equity $9,332.25 vs yday $9,261.85 (+70.40) | 09:30 open · cash $126.63 (unchanged overnight, no fees) · equity $9,332.25 vs prior close $9,261.85 (+70.40) because holdings re-marked: RVTY×8 yday $130.94 → 09:30 $132.45 +12.08; GPRO×903 yday $1.69 → 09:30 $1.78 +81.27; CRK×70 yday $15.54 → 09:30 $15.45 -6.30; MMED×48 yday $23.76 → 09:30 $23.88 +5.76; DEFT×1644 yday $0.65 → 09:30 $0.65 +0.00; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×66 yday $16.74 → 09:30 $16.77 +1.98; NVAX×107 yday $10.32 → 09:30 $10.41 +9.63 | — |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 9 | $1.95 | $0.20 | — | $108.88 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $18.09 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 5 | $3.57 | $0.19 | — | $90.83 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $18.09 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 3 | $5.08 | $0.16 | — | $75.43 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $18.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 6 | $2.70 | $0.18 | — | $59.05 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $18.09 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 1 | $13.30 | $0.14 | — | $45.62 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $18.09 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `QMLS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `CDNL` | cash | leftover split 0.45 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 0.45 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 0.45 < 1 share @ 31.30 |
| 2026-08-17 | `HTFL` | cash | leftover split 0.45 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 0.45 < 1 share @ 32.55 |
| 2026-08-17 | `NPWR` | cash | leftover split 0.45 < 1 share @ 1.92 |
| 2026-08-17 | `LPTH` | cash | leftover split 0.45 < 1 share @ 14.94 |
| 2026-08-17 | `NMAX` | cash | leftover split 0.45 < 1 share @ 10.97 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `QMLS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HTHT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `IVVD` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `CHRS` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `ABUS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 4.12 < 1 share @ 119.43 |
| 2026-08-21 | `AUPH` | cash | leftover split 4.12 < 1 share @ 17.20 |
| 2026-08-21 | `AEM` | cash | leftover split 4.12 < 1 share @ 216.30 |
| 2026-08-21 | `ARCT` | cash | leftover split 4.12 < 1 share @ 11.13 |
| 2026-08-21 | `DE` | cash | leftover split 4.12 < 1 share @ 623.26 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `ABUS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `GORO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BTBT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ASST` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `BMNR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `DEFT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ASST` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `BMNR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `ZYME` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `GENB` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `TIGR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `ZYME` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `GENB` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `TIGR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `NVAX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 18.09 < 1 share @ 486.31 |
| 2026-09-04 | `TARS` | cash | leftover split 18.09 < 1 share @ 82.76 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 8 | 2026-09-03 @ $125.94 | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1101.90 |
| `GPRO` | 903 | 2026-09-03 @ $1.22 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1101.90 |
| `CRK` | 70 | 2026-09-03 @ $15.70 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1101.90 |
| `MMED` | 48 | 2026-09-03 @ $22.78 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1101.90 |
| `DEFT` | 1644 | 2026-09-03 @ $0.67 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1101.90 |
| `MRNA` | 7 | 2026-09-03 @ $151.40 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1101.90 |
| `ARCT` | 66 | 2026-09-03 @ $16.46 | combo gate; gate vol=good,last_green=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1101.90 |
| `NVAX` | 107 | 2026-09-03 @ $10.27 | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1101.90 |
| `BAK` | 9 | 2026-09-04 @ $1.95 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $18.09 |
| `EOSE` | 5 | 2026-09-04 @ $3.57 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $18.09 |
| `OABI` | 3 | 2026-09-04 @ $5.08 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $18.09 |
| `ALEC` | 6 | 2026-09-04 @ $2.70 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $18.09 |
| `FMC` | 1 | 2026-09-04 @ $13.30 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $18.09 |
