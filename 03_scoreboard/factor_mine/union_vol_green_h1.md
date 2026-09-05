# Factor mine action — `union_vol_green_h1`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **1** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-6.39%** ($9,361) · signal-only (no cash/fees) was -11.83%. Starts YES **3/17**. Fills 118 · skips 44 · realized $-614.43.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,last_green=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **1**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $193.84.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | — | $3.57 | $9,801.97 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $3.57 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, AIRO×112, NCMI×464, QMLS×170 | $9,759.50 | -42.47 | CDNL, ABX, VERA, HTFL, UMAC, NPWR, LPTH, NMAX | BTBT, BETR, ANGX, HYLN, ADUR, AIRO, NCMI, QMLS | $71.87 | $9,428.97 | CDNL×30, ABX×133, VERA×38, HTFL×29, UMAC×37, NPWR×633, LPTH×81, NMAX×110 | 09:30 open · cash $3.57 (unchanged overnight, no fees) · equity $9,759.50 vs prior close $9,801.97 (-42.47) because holdings re-marked: BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84; QMLS×170 yday $7.32 → 09:30 $7.24 -13.60 |
| 2026-08-18 | -6.20 | $71.87 | CDNL×30, ABX×133, VERA×38, HTFL×29, UMAC×37, NPWR×633, LPTH×81, NMAX×110 | $9,316.08 | -112.89 | — | CDNL, ABX, VERA, HTFL, UMAC, NPWR, LPTH, NMAX | $9,292.33 | $9,292.33 | — | 09:30 open · cash $71.87 (unchanged overnight, no fees) · equity $9,316.08 vs prior close $9,428.97 (-112.89) because holdings re-marked: CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×133 yday $9.12 → 09:30 $9.03 -11.97; VERA×38 yday $31.63 → 09:30 $31.31 -12.16; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NPWR×633 yday $1.73 → 09:30 $1.70 -18.99; LPTH×81 yday $14.80 → 09:30 $14.01 -63.99; NMAX×110 yday $10.36 → 09:30 $10.31 -5.50 |
| 2026-08-19 | -7.20 | $9,292.33 | — | $9,292.33 | -0.00 | — | — | $9,292.33 | $9,292.33 | — | 09:30 open · cash $9,292.33 · no holdings · equity $9,292.33 vs prior close $9,292.33 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-20 | +1.12 | $9,292.33 | — | $9,292.33 | -0.00 | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | — | $9.13 | $9,420.74 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | 09:30 open · cash $9,292.33 · no holdings · equity $9,292.33 vs prior close $9,292.33 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $9.13 | AG×56, CDE×56, HDSN×201, IAG×59, KGC×39, NFGC×663, WPM×8, ABUS×236 | $9,748.43 | +327.69 | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, GORO | AG, CDE, HDSN, IAG, KGC, NFGC, WPM, ABUS | $727.79 | $9,969.77 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, DE×1, GORO×390 | 09:30 open · cash $9.13 (unchanged overnight, no fees) · equity $9,748.43 vs prior close $9,420.74 (+327.69) because holdings re-marked: AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×663 yday $1.75 → 09:30 $1.79 +26.52; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 |
| 2026-08-24 | -5.17 | $727.79 | AU×10, AUPH×70, AEM×5, ARCT×109, CYPH×920, BTBT×732, DE×1, GORO×390 | $10,345.10 | +375.33 | — | AU, AUPH, AEM, ARCT, CYPH, BTBT, DE, GORO | $10,307.74 | $10,307.74 | — | 09:30 open · cash $727.79 (unchanged overnight, no fees) · equity $10,345.10 vs prior close $9,969.77 (+375.33) because holdings re-marked: AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×70 yday $16.65 → 09:30 $16.60 -3.50; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×109 yday $13.45 → 09:30 $13.26 -20.71; CYPH×920 yday $1.42 → 09:30 $1.83 +377.20; BTBT×732 yday $1.53 → 09:30 $1.55 +14.64; DE×1 yday $647.47 → 09:30 $653.62 +6.15; GORO×390 yday $3.19 → 09:30 $3.20 +3.90 |
| 2026-08-25 | +1.80 | $10,307.74 | — | $10,307.74 | +0.00 | NPWR, ALVO, ZURA, CYPH, DEFT, GORO, ASST, BMNR | — | $1.65 | $10,156.38 | NPWR×644, ALVO×246, ZURA×201, CYPH×757, DEFT×2013, GORO×365, ASST×61, BMNR×51 | 09:30 open · cash $10,307.74 · no holdings · equity $10,307.74 vs prior close $10,307.74 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-26 | +2.02 | $1.65 | NPWR×644, ALVO×246, ZURA×201, CYPH×757, DEFT×2013, GORO×365, ASST×61, BMNR×51 | $10,156.38 | +0.00 | — | — | $1.65 | $10,255.95 | NPWR×644, ALVO×246, ZURA×201, CYPH×757, DEFT×2013, GORO×365, ASST×61, BMNR×51 | 09:30 open · cash $1.65 (unchanged overnight, no fees) · equity $10,156.38 vs prior close $10,156.38 (+0.00) because holdings re-marked: NPWR×644 yday $2.02 → 09:30 $2.02 +0.00; ALVO×246 yday $5.25 → 09:30 $5.25 +0.00; ZURA×201 yday $6.50 → 09:30 $6.50 +0.00; CYPH×757 yday $1.64 → 09:30 $1.64 +0.00; DEFT×2013 yday $0.62 → 09:30 $0.62 +0.00; GORO×365 yday $3.56 → 09:30 $3.56 +0.00; ASST×61 yday $20.20 → 09:30 $20.20 +0.00; BMNR×51 yday $24.21 → 09:30 $24.21 +0.00 |
| 2026-08-27 | — | $1.65 | NPWR×644, ALVO×246, ZURA×201, CYPH×757, DEFT×2013, GORO×365, ASST×61, BMNR×51 | $9,996.99 | -258.96 | — | NPWR, ALVO, ZURA, CYPH, DEFT, GORO, ASST, BMNR | $9,945.20 | $9,945.20 | — | 09:30 open · cash $1.65 (unchanged overnight, no fees) · equity $9,996.99 vs prior close $10,255.95 (-258.96) because holdings re-marked: NPWR×644 yday $2.02 → 09:30 $1.93 -57.96; ALVO×246 yday $5.25 → 09:30 $4.98 -66.42; ZURA×201 yday $6.50 → 09:30 $6.13 -74.37; CYPH×757 yday $1.64 → 09:30 $1.60 -30.28; DEFT×2013 yday $0.62 → 09:30 $0.60 -40.26; GORO×365 yday $3.56 → 09:30 $3.77 +76.65; ASST×61 yday $20.20 → 09:30 $20.72 +31.72; BMNR×51 yday $24.21 → 09:30 $24.24 +1.53 |
| 2026-08-28 | +0.75 | $9,945.20 | — | $9,945.20 | +0.00 | ANF, BHVN, BZ, URBN, ERAS, ZYME, GENB, TIGR | — | $113.38 | $9,588.77 | ANF×8, BHVN×73, BZ×67, URBN×15, ERAS×64, ZYME×42, GENB×72, TIGR×226 | 09:30 open · cash $9,945.20 · no holdings · equity $9,945.20 vs prior close $9,945.20 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-31 | -5.85 | $113.38 | ANF×8, BHVN×73, BZ×67, URBN×15, ERAS×64, ZYME×42, GENB×72, TIGR×226 | $9,402.50 | -186.27 | — | ANF, BHVN, BZ, URBN, ERAS, ZYME, GENB, TIGR | $9,384.44 | $9,384.44 | — | 09:30 open · cash $113.38 (unchanged overnight, no fees) · equity $9,402.50 vs prior close $9,588.77 (-186.27) because holdings re-marked: ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×73 yday $16.12 → 09:30 $15.44 -49.64; BZ×67 yday $18.00 → 09:30 $17.89 -7.37; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; ERAS×64 yday $19.49 → 09:30 $17.90 -101.76; ZYME×42 yday $29.01 → 09:30 $28.27 -31.08; GENB×72 yday $15.77 → 09:30 $15.33 -31.68; TIGR×226 yday $5.06 → 09:30 $4.96 -22.60 |
| 2026-09-01 | -6.30 | $9,384.44 | — | $9,384.44 | +0.00 | — | — | $9,384.44 | $9,384.44 | — | 09:30 open · cash $9,384.44 · no holdings · equity $9,384.44 vs prior close $9,384.44 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-02 | -3.83 | $9,384.44 | — | $9,384.44 | +0.00 | — | — | $9,384.44 | $9,384.44 | — | 09:30 open · cash $9,384.44 · no holdings · equity $9,384.44 vs prior close $9,384.44 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-03 | -0.90 | $9,384.44 | — | $9,384.44 | +0.00 | RVTY, GPRO, CRK, MMED, DEFT, MRNA, ARCT, NVAX | — | $140.95 | $9,863.41 | RVTY×9, GPRO×961, CRK×74, MMED×51, DEFT×1750, MRNA×7, ARCT×71, NVAX×114 | 09:30 open · cash $9,384.44 · no holdings · equity $9,384.44 vs prior close $9,384.44 (+0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $140.95 | RVTY×9, GPRO×961, CRK×74, MMED×51, DEFT×1750, MRNA×7, ARCT×71, NVAX×114 | $9,941.32 | +77.91 | BAK, EOSE, DELL, OABI, ALEC, FMC, TARS | RVTY, CRK, MMED, DEFT, MRNA, ARCT, NVAX | $193.84 | $9,360.92 | GPRO×961, BAK×600, EOSE×328, DELL×2, OABI×230, ALEC×433, FMC×88, TARS×14 | 09:30 open · cash $140.95 (unchanged overnight, no fees) · equity $9,941.32 vs prior close $9,863.41 (+77.91) because holdings re-marked: RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×961 yday $1.69 → 09:30 $1.78 +86.49; CRK×74 yday $15.54 → 09:30 $15.45 -6.66; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; DEFT×1750 yday $0.65 → 09:30 $0.65 +0.00; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×71 yday $16.74 → 09:30 $16.77 +2.13; NVAX×114 yday $10.32 → 09:30 $10.41 +10.26 |

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
| 2026-08-17 09:30 ET | **SELL** | `BTBT` | 833 | $1.52 | $10.89 | $-4.98 | $1,258.83 | ▼ -4.98 after sell → book $9,748.60; vs 09:30 mark -10.90 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `BETR` | 84 | $13.67 | $2.27 | $-99.43 | $2,404.85 | ▼ -99.43 after sell → book $9,746.34; vs 09:30 mark -2.26 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ANGX` | 290 | $4.60 | $3.80 | $+76.56 | $3,735.05 | ▲ +76.56 after sell → book $9,742.54; vs 09:30 mark -3.80 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `HYLN` | 299 | $4.10 | $3.92 | $-31.69 | $4,957.03 | ▼ -31.69 after sell → book $9,738.62; vs 09:30 mark -3.92 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `ADUR` | 75 | $15.73 | $2.24 | $-62.20 | $6,134.54 | ▼ -62.20 after sell → book $9,736.38; vs 09:30 mark -2.24 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `AIRO` | 112 | $9.57 | $2.35 | $-178.28 | $7,204.03 | ▼ -178.28 after sell → book $9,734.03; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `NCMI` | 464 | $2.80 | $6.07 | $+38.98 | $8,497.16 | ▲ +38.98 after sell → book $9,727.96; vs 09:30 mark -6.07 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **SELL** | `QMLS` | 170 | $7.24 | $2.54 | $-13.54 | $9,725.42 | ▼ -13.54 after sell → book $9,725.42; vs 09:30 mark -2.54 | dropped from list after 1 sess (min 1) | — |
| 2026-08-17 09:30 ET | **BUY** | `CDNL` | 30 | $39.85 | $2.08 | — | $8,527.84 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ⚪; ret5=-38.4; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `ABX` | 133 | $9.12 | $2.39 | — | $7,312.49 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=-2.6; leftover $1215.68 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `VERA` | 38 | $31.30 | $2.10 | — | $6,120.98 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=-3.8; leftover $1215.68 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `HTFL` | 29 | $41.23 | $2.08 | — | $4,923.24 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+46.0; leftover $1215.68 | join🟡 sector🔴 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `UMAC` | 37 | $32.55 | $2.10 | — | $3,716.79 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+30.4; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `NPWR` | 633 | $1.92 | $8.17 | — | $2,493.26 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+28.3; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **BUY** | `LPTH` | 81 | $14.94 | $2.23 | — | $1,280.89 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; ⚪; ret5=+16.2; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟢 |
| 2026-08-17 09:30 ET | **BUY** | `NMAX` | 110 | $10.97 | $2.32 | — | $71.87 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ⚪; ret5=+21.2; leftover $1215.68 | join🟡 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $71.87 | ▼ 09:30 equity $9,316.08 vs yday $9,428.97 (-112.89) | 09:30 open · cash $71.87 (unchanged overnight, no fees) · equity $9,316.08 vs prior close $9,428.97 (-112.89) because holdings re-marked: CDNL×30 yday $39.23 → 09:30 $41.57 +70.20; ABX×133 yday $9.12 → 09:30 $9.03 -11.97; VERA×38 yday $31.63 → 09:30 $31.31 -12.16; HTFL×29 yday $41.94 → 09:30 $41.50 -12.76; UMAC×37 yday $30.15 → 09:30 $28.59 -57.72; NPWR×633 yday $1.73 → 09:30 $1.70 -18.99; LPTH×81 yday $14.80 → 09:30 $14.01 -63.99; NMAX×110 yday $10.36 → 09:30 $10.31 -5.50 | — |
| 2026-08-18 09:30 ET | **SELL** | `CDNL` | 30 | $41.57 | $2.10 | $+47.42 | $1,316.87 | ▲ +47.42 after sell → book $9,313.98; vs 09:30 mark -2.10 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `ABX` | 133 | $9.03 | $2.42 | $-16.78 | $2,515.44 | ▼ -16.78 after sell → book $9,311.56; vs 09:30 mark -2.42 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `VERA` | 38 | $31.31 | $2.12 | $-3.85 | $3,703.09 | ▼ -3.85 after sell → book $9,309.43; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `HTFL` | 29 | $41.50 | $2.10 | $+3.66 | $4,904.50 | ▲ +3.66 after sell → book $9,307.34; vs 09:30 mark -2.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `UMAC` | 37 | $28.59 | $2.12 | $-150.74 | $5,960.21 | ▼ -150.74 after sell → book $9,305.22; vs 09:30 mark -2.12 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NPWR` | 633 | $1.70 | $8.28 | $-155.71 | $7,028.02 | ▼ -155.71 after sell → book $9,296.93; vs 09:30 mark -8.29 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `LPTH` | 81 | $14.01 | $2.26 | $-79.82 | $8,160.58 | ▼ -79.82 after sell → book $9,294.68; vs 09:30 mark -2.25 | dropped from list after 1 sess (min 1) | — |
| 2026-08-18 09:30 ET | **SELL** | `NMAX` | 110 | $10.31 | $2.35 | $-77.27 | $9,292.33 | ▼ -77.27 after sell → book $9,292.33; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🔴 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,292.33 | ▲ 09:30 equity $9,292.33 vs yday $9,292.33 (-0.00) | 09:30 open · cash $9,292.33 · no holdings · equity $9,292.33 vs prior close $9,292.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,292.33 | ▲ 09:30 equity $9,292.33 vs yday $9,292.33 (-0.00) | 09:30 open · cash $9,292.33 · no holdings · equity $9,292.33 vs prior close $9,292.33 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 56 | $20.55 | $2.16 | — | $8,139.37 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $6,980.81 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 201 | $5.77 | $2.60 | — | $5,818.45 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $4,658.11 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $3,500.43 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 663 | $1.75 | $8.55 | — | $2,331.63 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $1,173.29 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `ABUS` | 236 | $4.92 | $3.04 | — | $9.13 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.5; leftover $1161.54 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9.13 | ▲ 09:30 equity $9,748.43 vs yday $9,420.74 (+327.69) | 09:30 open · cash $9.13 (unchanged overnight, no fees) · equity $9,748.43 vs prior close $9,420.74 (+327.69) because holdings re-marked: AG×56 yday $21.19 → 09:30 $21.90 +39.76; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×201 yday $5.57 → 09:30 $5.67 +20.10; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×663 yday $1.75 → 09:30 $1.79 +26.52; WPM×8 yday $150.25 → 09:30 $154.70 +35.60; ABUS×236 yday $4.77 → 09:30 $5.20 +101.48 | — |
| 2026-08-21 09:30 ET | **SELL** | `AG` | 56 | $21.90 | $2.18 | $+71.26 | $1,233.35 | ▲ +71.26 after sell → book $9,746.25; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `CDE` | 56 | $21.75 | $2.18 | $+57.26 | $2,449.17 | ▲ +57.26 after sell → book $9,744.07; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `HDSN` | 201 | $5.67 | $2.64 | $-25.34 | $3,586.20 | ▼ -25.34 after sell → book $9,741.43; vs 09:30 mark -2.64 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `IAG` | 59 | $21.17 | $2.19 | $+86.51 | $4,833.05 | ▲ +86.51 after sell → book $9,739.25; vs 09:30 mark -2.18 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `KGC` | 39 | $32.17 | $2.13 | $+94.83 | $6,085.55 | ▲ +94.83 after sell → book $9,737.12; vs 09:30 mark -2.13 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `NFGC` | 663 | $1.79 | $8.67 | $+9.29 | $7,263.65 | ▲ +9.29 after sell → book $9,728.45; vs 09:30 mark -8.67 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **SELL** | `WPM` | 8 | $154.70 | $2.03 | $+77.23 | $8,499.21 | ▲ +77.23 after sell → book $9,726.41; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟡 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `ABUS` | 236 | $5.20 | $3.09 | $+59.94 | $9,723.32 | ▲ +59.94 after sell → book $9,723.32; vs 09:30 mark -3.09 | dropped from list after 1 sess (min 1) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 10 | $119.43 | $2.02 | — | $8,527.00 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 70 | $17.20 | $2.20 | — | $7,320.80 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $6,237.29 | — | combo gate; gate vol=good,last_green=True; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 109 | $11.13 | $2.32 | — | $5,021.81 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 920 | $1.32 | $11.87 | — | $3,795.54 | — | combo gate; gate vol=good,last_green=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `BTBT` | 732 | $1.66 | $9.44 | — | $2,570.98 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+7.0; leftover $1215.41 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `DE` | 1 | $623.26 | $1.99 | — | $1,945.72 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ret5=+1.4; leftover $1215.41 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `GORO` | 390 | $3.11 | $5.03 | — | $727.79 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; ret5=+7.1; leftover $1215.41 | join🟡 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $727.79 | ▲ 09:30 equity $10,345.10 vs yday $9,969.77 (+375.33) | 09:30 open · cash $727.79 (unchanged overnight, no fees) · equity $10,345.10 vs prior close $9,969.77 (+375.33) because holdings re-marked: AU×10 yday $121.22 → 09:30 $120.50 -7.20; AUPH×70 yday $16.65 → 09:30 $16.60 -3.50; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×109 yday $13.45 → 09:30 $13.26 -20.71; CYPH×920 yday $1.42 → 09:30 $1.83 +377.20; BTBT×732 yday $1.53 → 09:30 $1.55 +14.64; DE×1 yday $647.47 → 09:30 $653.62 +6.15; GORO×390 yday $3.19 → 09:30 $3.20 +3.90 | — |
| 2026-08-24 09:30 ET | **SELL** | `AU` | 10 | $120.50 | $2.04 | $+6.64 | $1,930.75 | ▲ +6.64 after sell → book $10,343.06; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AUPH` | 70 | $16.60 | $2.22 | $-46.42 | $3,090.53 | ▼ -46.42 after sell → book $10,340.84; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `AEM` | 5 | $217.03 | $2.02 | $-0.38 | $4,173.66 | ▼ -0.38 after sell → book $10,338.82; vs 09:30 mark -2.02 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 judge🟡 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `ARCT` | 109 | $13.26 | $2.35 | $+227.51 | $5,616.65 | ▲ +227.51 after sell → book $10,336.47; vs 09:30 mark -2.35 | dropped from list after 1 sess (min 1) | join🔴 sector🟡 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **SELL** | `CYPH` | 920 | $1.83 | $12.03 | $+445.30 | $7,288.21 | ▲ +445.30 after sell → book $10,324.43; vs 09:30 mark -12.04 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `BTBT` | 732 | $1.55 | $9.57 | $-99.54 | $8,413.24 | ▼ -99.54 after sell → book $10,314.86; vs 09:30 mark -9.57 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `DE` | 1 | $653.62 | $2.01 | $+26.35 | $9,064.85 | ▲ +26.35 after sell → book $10,312.85; vs 09:30 mark -2.01 | dropped from list after 1 sess (min 1) | — |
| 2026-08-24 09:30 ET | **SELL** | `GORO` | 390 | $3.20 | $5.11 | $+24.96 | $10,307.74 | ▲ +24.96 after sell → book $10,307.74; vs 09:30 mark -5.11 | dropped from list after 1 sess (min 1) | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,307.74 | ▲ 09:30 equity $10,307.74 vs yday $10,307.74 (+0.00) | 09:30 open · cash $10,307.74 · no holdings · equity $10,307.74 vs prior close $10,307.74 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 644 | $2.00 | $8.31 | — | $9,011.43 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1288.47 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 246 | $5.22 | $3.17 | — | $7,724.14 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1288.47 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 201 | $6.38 | $2.60 | — | $6,439.16 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1288.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CYPH` | 757 | $1.70 | $9.77 | — | $5,142.50 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+120.5; leftover $1288.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `DEFT` | 2013 | $0.64 | $18.92 | — | $3,835.25 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ⚪; ret5=+17.6; leftover $1288.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `GORO` | 365 | $3.53 | $4.71 | — | $2,542.10 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+16.0; leftover $1288.47 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ASST` | 61 | $20.90 | $2.17 | — | $1,265.02 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ret5=+47.9; leftover $1288.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMNR` | 51 | $24.73 | $2.14 | — | $1.65 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=+26.3; leftover $1288.47 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🟡 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.65 | ▲ 09:30 equity $10,156.38 vs yday $10,156.38 (+0.00) | 09:30 open · cash $1.65 (unchanged overnight, no fees) · equity $10,156.38 vs prior close $10,156.38 (+0.00) because holdings re-marked: NPWR×644 yday $2.02 → 09:30 $2.02 +0.00; ALVO×246 yday $5.25 → 09:30 $5.25 +0.00; ZURA×201 yday $6.50 → 09:30 $6.50 +0.00; CYPH×757 yday $1.64 → 09:30 $1.64 +0.00; DEFT×2013 yday $0.62 → 09:30 $0.62 +0.00; GORO×365 yday $3.56 → 09:30 $3.56 +0.00; ASST×61 yday $20.20 → 09:30 $20.20 +0.00; BMNR×51 yday $24.21 → 09:30 $24.21 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $1.65 | ▼ 09:30 equity $9,996.99 vs yday $10,255.95 (-258.96) | 09:30 open · cash $1.65 (unchanged overnight, no fees) · equity $9,996.99 vs prior close $10,255.95 (-258.96) because holdings re-marked: NPWR×644 yday $2.02 → 09:30 $1.93 -57.96; ALVO×246 yday $5.25 → 09:30 $4.98 -66.42; ZURA×201 yday $6.50 → 09:30 $6.13 -74.37; CYPH×757 yday $1.64 → 09:30 $1.60 -30.28; DEFT×2013 yday $0.62 → 09:30 $0.60 -40.26; GORO×365 yday $3.56 → 09:30 $3.77 +76.65; ASST×61 yday $20.20 → 09:30 $20.72 +31.72; BMNR×51 yday $24.21 → 09:30 $24.24 +1.53 | — |
| 2026-08-27 09:30 ET | **SELL** | `NPWR` | 644 | $1.93 | $8.42 | $-61.81 | $1,236.15 | ▼ -61.81 after sell → book $9,988.57; vs 09:30 mark -8.42 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ALVO` | 246 | $4.98 | $3.22 | $-65.44 | $2,458.00 | ▼ -65.44 after sell → book $9,985.34; vs 09:30 mark -3.23 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ZURA` | 201 | $6.13 | $2.64 | $-55.49 | $3,687.49 | ▼ -55.49 after sell → book $9,982.70; vs 09:30 mark -2.64 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 757 | $1.60 | $9.90 | $-95.37 | $4,888.79 | ▼ -95.37 after sell → book $9,972.80; vs 09:30 mark -9.90 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `DEFT` | 2013 | $0.60 | $18.46 | $-117.90 | $6,078.13 | ▼ -117.90 after sell → book $9,954.34; vs 09:30 mark -18.46 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `GORO` | 365 | $3.77 | $4.78 | $+78.11 | $7,449.40 | ▲ +78.11 after sell → book $9,949.56; vs 09:30 mark -4.78 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `ASST` | 61 | $20.72 | $2.19 | $-15.35 | $8,711.12 | ▼ -15.35 after sell → book $9,947.36; vs 09:30 mark -2.20 | dropped from list after 2 sess (min 1) | — |
| 2026-08-27 09:30 ET | **SELL** | `BMNR` | 51 | $24.24 | $2.16 | $-29.30 | $9,945.20 | ▼ -29.30 after sell → book $9,945.20; vs 09:30 mark -2.16 | dropped from list after 2 sess (min 1) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,945.20 | ▲ 09:30 equity $9,945.20 vs yday $9,945.20 (+0.00) | 09:30 open · cash $9,945.20 · no holdings · equity $9,945.20 vs prior close $9,945.20 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 8 | $144.70 | $2.01 | — | $8,785.59 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1243.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 73 | $16.95 | $2.21 | — | $7,546.03 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1243.15 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 67 | $18.50 | $2.19 | — | $6,304.34 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1243.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 15 | $82.70 | $2.04 | — | $5,061.80 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1243.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 64 | $19.30 | $2.18 | — | $3,824.42 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; ret5=-4.1; leftover $1243.15 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ZYME` | 42 | $29.33 | $2.12 | — | $2,590.44 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot; ret5=+14.1; leftover $1243.15 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `GENB` | 72 | $17.10 | $2.21 | — | $1,357.04 | — | combo gate; gate vol=good,last_green=True; list yday_mover; ret5=+3.1; leftover $1243.15 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟡 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `TIGR` | 226 | $5.49 | $2.92 | — | $113.38 | — | combo gate; gate vol=good,last_green=True; list ohlc_hot; ret5=+15.9; leftover $1243.15 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟢 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $113.38 | ▼ 09:30 equity $9,402.50 vs yday $9,588.77 (-186.27) | 09:30 open · cash $113.38 (unchanged overnight, no fees) · equity $9,402.50 vs prior close $9,588.77 (-186.27) because holdings re-marked: ANF×8 yday $145.75 → 09:30 $148.67 +23.36; BHVN×73 yday $16.12 → 09:30 $15.44 -49.64; BZ×67 yday $18.00 → 09:30 $17.89 -7.37; URBN×15 yday $78.79 → 09:30 $81.09 +34.50; ERAS×64 yday $19.49 → 09:30 $17.90 -101.76; ZYME×42 yday $29.01 → 09:30 $28.27 -31.08; GENB×72 yday $15.77 → 09:30 $15.33 -31.68; TIGR×226 yday $5.06 → 09:30 $4.96 -22.60 | — |
| 2026-08-31 09:30 ET | **SELL** | `ANF` | 8 | $148.67 | $2.03 | $+27.71 | $1,300.71 | ▲ +27.71 after sell → book $9,400.47; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BHVN` | 73 | $15.44 | $2.23 | $-114.67 | $2,425.60 | ▼ -114.67 after sell → book $9,398.24; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `BZ` | 67 | $17.89 | $2.21 | $-45.27 | $3,622.02 | ▼ -45.27 after sell → book $9,396.03; vs 09:30 mark -2.21 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `URBN` | 15 | $81.09 | $2.06 | $-28.24 | $4,836.31 | ▼ -28.24 after sell → book $9,393.97; vs 09:30 mark -2.06 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ERAS` | 64 | $17.90 | $2.20 | $-93.98 | $5,979.71 | ▼ -93.98 after sell → book $9,391.77; vs 09:30 mark -2.20 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `ZYME` | 42 | $28.27 | $2.14 | $-48.77 | $7,164.91 | ▼ -48.77 after sell → book $9,389.63; vs 09:30 mark -2.14 | dropped from list after 1 sess (min 1) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟡 |
| 2026-08-31 09:30 ET | **SELL** | `GENB` | 72 | $15.33 | $2.23 | $-131.87 | $8,266.44 | ▼ -131.87 after sell → book $9,387.40; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-08-31 09:30 ET | **SELL** | `TIGR` | 226 | $4.96 | $2.96 | $-125.66 | $9,384.44 | ▼ -125.66 after sell → book $9,384.44; vs 09:30 mark -2.96 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🔴 news🟡 digest🟢 ab🟢 peer🟢 heat🔴 vol🟡 buy🟢 |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,384.44 | ▲ 09:30 equity $9,384.44 vs yday $9,384.44 (+0.00) | 09:30 open · cash $9,384.44 · no holdings · equity $9,384.44 vs prior close $9,384.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,384.44 | ▲ 09:30 equity $9,384.44 vs yday $9,384.44 (+0.00) | 09:30 open · cash $9,384.44 · no holdings · equity $9,384.44 vs prior close $9,384.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,384.44 | ▲ 09:30 equity $9,384.44 vs yday $9,384.44 (+0.00) | 09:30 open · cash $9,384.44 · no holdings · equity $9,384.44 vs prior close $9,384.44 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,248.96 | — | combo gate; gate vol=good,last_green=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 961 | $1.22 | $12.40 | — | $7,064.15 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1173.06 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 74 | $15.70 | $2.21 | — | $5,900.14 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1173.06 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 51 | $22.78 | $2.14 | — | $4,736.21 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1750 | $0.67 | $16.98 | — | $3,546.74 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1173.06 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $2,484.93 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 71 | $16.46 | $2.20 | — | $1,314.06 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `NVAX` | 114 | $10.27 | $2.33 | — | $140.95 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+11.1; leftover $1173.06 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $140.95 | ▲ 09:30 equity $9,941.32 vs yday $9,863.41 (+77.91) | 09:30 open · cash $140.95 (unchanged overnight, no fees) · equity $9,941.32 vs prior close $9,863.41 (+77.91) because holdings re-marked: RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; GPRO×961 yday $1.69 → 09:30 $1.78 +86.49; CRK×74 yday $15.54 → 09:30 $15.45 -6.66; MMED×51 yday $23.76 → 09:30 $23.88 +6.12; DEFT×1750 yday $0.65 → 09:30 $0.65 +0.00; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×71 yday $16.74 → 09:30 $16.77 +2.13; NVAX×114 yday $10.32 → 09:30 $10.41 +10.26 | — |
| 2026-09-04 09:30 ET | **SELL** | `RVTY` | 9 | $132.45 | $2.04 | $+54.54 | $1,330.96 | ▲ +54.54 after sell → book $9,939.28; vs 09:30 mark -2.04 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `CRK` | 74 | $15.45 | $2.23 | $-22.95 | $2,472.03 | ▼ -22.95 after sell → book $9,937.05; vs 09:30 mark -2.23 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `MMED` | 51 | $23.88 | $2.16 | $+51.79 | $3,687.75 | ▲ +51.79 after sell → book $9,934.89; vs 09:30 mark -2.16 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `DEFT` | 1750 | $0.65 | $16.93 | $-68.90 | $4,808.32 | ▼ -68.90 after sell → book $9,917.96; vs 09:30 mark -16.93 | dropped from list after 1 sess (min 1) | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🔴 buy🟡 |
| 2026-09-04 09:30 ET | **SELL** | `MRNA` | 7 | $145.95 | $2.03 | $-42.19 | $5,827.94 | ▼ -42.19 after sell → book $9,915.93; vs 09:30 mark -2.03 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `ARCT` | 71 | $16.77 | $2.22 | $+17.58 | $7,016.39 | ▲ +17.58 after sell → book $9,913.71; vs 09:30 mark -2.22 | dropped from list after 1 sess (min 1) | — |
| 2026-09-04 09:30 ET | **SELL** | `NVAX` | 114 | $10.41 | $2.36 | $+11.27 | $8,200.77 | ▲ +11.27 after sell → book $9,911.35; vs 09:30 mark -2.36 | dropped from list after 1 sess (min 1) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟡 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 600 | $1.95 | $7.74 | — | $7,023.03 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1171.54 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 328 | $3.57 | $4.23 | — | $5,847.83 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1171.54 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,873.22 | — | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1171.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 230 | $5.08 | $2.97 | — | $3,701.85 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1171.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `ALEC` | 433 | $2.70 | $5.59 | — | $2,527.17 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1171.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `FMC` | 88 | $13.30 | $2.25 | — | $1,354.51 | — | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $1171.54 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `TARS` | 14 | $82.76 | $2.03 | — | $193.84 | — | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1171.54 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `ANGX` | hard_red | hard-red S=-6.20 sit; no new buys |
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
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-26 | `NPWR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ALVO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ZURA` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `DEFT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `GORO` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ASST` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMNR` | no_price | no 09:30 open — carry |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `TENB` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `RBRK` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CAN` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `PURR` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 961 | 2026-09-03 @ $1.22 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $1173.06 |
| `BAK` | 600 | 2026-09-04 @ $1.95 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1171.54 |
| `EOSE` | 328 | 2026-09-04 @ $3.57 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1171.54 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | combo gate; gate vol=good,last_green=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1171.54 |
| `OABI` | 230 | 2026-09-04 @ $5.08 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $1171.54 |
| `ALEC` | 433 | 2026-09-04 @ $2.70 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+20.4; leftover $1171.54 |
| `FMC` | 88 | 2026-09-04 @ $13.30 | combo gate; gate vol=good,last_green=True; list yday_gainer,yday_mover; ret5=+8.6; leftover $1171.54 |
| `TARS` | 14 | 2026-09-04 @ $82.76 | combo gate; gate vol=good,last_green=True; list yday_gainer; 🔵; ⚪; ret5=+5.1; leftover $1171.54 |
