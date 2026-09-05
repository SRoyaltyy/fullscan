# Factor mine action — `union_blue_vol_h3`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **3** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · combo gate

Cash book **-5.07%** ($9,493) · signal-only (no cash/fees) was -11.54%. Starts YES **3/17**. Fills 78 · skips 114 · realized $-420.58.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good,blue=True` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **3**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $64.18.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,768.32 | -29.50 | — | — | $10.28 | $9,809.66 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) because holdings re-marked: BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 |
| 2026-08-18 | -6.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,628.11 | -181.55 | — | — | $10.28 | $9,454.54 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,628.11 vs prior close $9,809.66 (-181.55) because holdings re-marked: BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28 |
| 2026-08-19 | -7.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,448.22 | -6.32 | — | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | $9,414.48 | $9,414.48 | — | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,448.22 vs prior close $9,454.54 (-6.32) because holdings re-marked: BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56 |
| 2026-08-20 | +1.12 | $9,414.48 | — | $9,414.48 | -0.00 | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | — | $153.32 | $9,610.85 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8 | 09:30 open · cash $9,414.48 · no holdings · equity $9,414.48 vs prior close $9,414.48 (-0.00). Cash unchanged overnight; no fees. |
| 2026-08-21 | +3.25 | $153.32 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8 | $9,863.41 | +252.56 | AUPH, ARCT, AUTL, CRDL, CYPH | — | $70.94 | $9,861.85 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8, AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14 | 09:30 open · cash $153.32 (unchanged overnight, no fees) · equity $9,863.41 vs prior close $9,610.85 (+252.56) because holdings re-marked: AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×203 yday $5.57 → 09:30 $5.67 +20.30; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×672 yday $1.75 → 09:30 $1.79 +26.88; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 |
| 2026-08-24 | -5.17 | $70.94 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8, AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14 | $9,979.02 | +117.17 | — | — | $70.94 | $9,835.05 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8, AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14 | 09:30 open · cash $70.94 (unchanged overnight, no fees) · equity $9,979.02 vs prior close $9,861.85 (+117.17) because holdings re-marked: AG×57 yday $21.09 → 09:30 $21.47 +21.66; BHP×12 yday $97.03 → 09:30 $97.34 +3.72; CDE×56 yday $20.97 → 09:30 $21.26 +16.24; HDSN×203 yday $5.63 → 09:30 $5.69 +12.18; IAG×59 yday $21.14 → 09:30 $21.44 +17.70; KGC×39 yday $32.76 → 09:30 $33.21 +17.55; NFGC×672 yday $1.84 → 09:30 $1.86 +13.44; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×7 yday $2.41 → 09:30 $2.36 -0.35; CRDL×9 yday $1.86 → 09:30 $1.87 +0.09; CYPH×14 yday $1.42 → 09:30 $1.83 +5.74 |
| 2026-08-25 | +1.80 | $70.94 | AG×57, BHP×12, CDE×56, HDSN×203, IAG×59, KGC×39, NFGC×672, WPM×8, AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14 | $9,899.66 | +64.61 | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA | AG, BHP, CDE, HDSN, IAG, KGC, NFGC, WPM | $5.76 | $10,000.04 | AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14, BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | 09:30 open · cash $70.94 (unchanged overnight, no fees) · equity $9,899.66 vs prior close $9,835.05 (+64.61) because holdings re-marked: AG×57 yday $20.57 → 09:30 $20.73 +9.12; BHP×12 yday $96.66 → 09:30 $95.95 -8.52; CDE×56 yday $20.49 → 09:30 $20.85 +20.16; HDSN×203 yday $5.57 → 09:30 $5.53 -8.12; IAG×59 yday $21.36 → 09:30 $21.63 +15.93; KGC×39 yday $32.47 → 09:30 $32.76 +11.31; NFGC×672 yday $1.90 → 09:30 $1.91 +6.72; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; AUTL×7 yday $2.38 → 09:30 $2.32 -0.42; CRDL×9 yday $1.80 → 09:30 $1.90 +0.90; CYPH×14 yday $1.64 → 09:30 $1.70 +0.84 |
| 2026-08-26 | +2.02 | $5.76 | AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14, BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | $10,000.04 | +0.00 | — | — | $5.76 | $9,839.06 | AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14, BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | 09:30 open · cash $5.76 (unchanged overnight, no fees) · equity $10,000.04 vs prior close $10,000.04 (+0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; AUTL×7 yday $2.34 → 09:30 $2.34 +0.00; CRDL×9 yday $1.90 → 09:30 $1.90 +0.00; CYPH×14 yday $1.64 → 09:30 $1.64 +0.00; BMEA×863 yday $1.61 → 09:30 $1.61 +0.00; NPWR×699 yday $2.02 → 09:30 $2.02 +0.00; PUSA×377 yday $3.91 → 09:30 $3.91 +0.00; ALVO×267 yday $5.25 → 09:30 $5.25 +0.00; CAPR×205 yday $7.19 → 09:30 $7.19 +0.00; ZURA×219 yday $6.50 → 09:30 $6.50 +0.00; SUJA×156 yday $8.54 → 09:30 $8.54 +0.00 |
| 2026-08-27 | — | $5.76 | AUPH×1, ARCT×1, AUTL×7, CRDL×9, CYPH×14, BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | $10,238.67 | +399.61 | — | AUPH, ARCT, AUTL, CRDL, CYPH | $94.16 | $10,300.75 | BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | 09:30 open · cash $5.76 (unchanged overnight, no fees) · equity $10,238.67 vs prior close $9,839.06 (+399.61) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; AUTL×7 yday $2.34 → 09:30 $2.41 +0.49; CRDL×9 yday $1.90 → 09:30 $2.03 +1.17; CYPH×14 yday $1.64 → 09:30 $1.60 -0.56; BMEA×863 yday $1.61 → 09:30 $1.75 +120.82; NPWR×699 yday $2.02 → 09:30 $1.93 -62.91; PUSA×377 yday $3.91 → 09:30 $3.84 -26.39; ALVO×267 yday $5.25 → 09:30 $4.98 -72.09; CAPR×205 yday $7.19 → 09:30 $8.29 +225.50; ZURA×219 yday $6.50 → 09:30 $6.13 -81.03; SUJA×156 yday $8.54 → 09:30 $9.39 +132.60 |
| 2026-08-28 | +0.75 | $94.16 | BMEA×863, NPWR×699, PUSA×377, ALVO×267, CAPR×205, ZURA×219, SUJA×156 | $10,303.42 | +2.67 | ANF, SEDG, SMTC, URBN | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA | $161.21 | $10,016.01 | ANF×17, SEDG×75, SMTC×17, URBN×31 | 09:30 open · cash $94.16 (unchanged overnight, no fees) · equity $10,303.42 vs prior close $10,300.75 (+2.67) because holdings re-marked: BMEA×863 yday $1.71 → 09:30 $1.74 +25.89; NPWR×699 yday $1.81 → 09:30 $1.83 +13.98; PUSA×377 yday $3.85 → 09:30 $3.86 +3.77; ALVO×267 yday $4.91 → 09:30 $4.88 -8.01; CAPR×205 yday $9.36 → 09:30 $9.19 -34.85; ZURA×219 yday $5.99 → 09:30 $6.02 +6.57; SUJA×156 yday $9.44 → 09:30 $9.41 -4.68 |
| 2026-08-31 | -5.85 | $161.21 | ANF×17, SEDG×75, SMTC×17, URBN×31 | $9,826.57 | -189.44 | — | — | $161.21 | $9,811.19 | ANF×17, SEDG×75, SMTC×17, URBN×31 | 09:30 open · cash $161.21 (unchanged overnight, no fees) · equity $9,826.57 vs prior close $10,016.01 (-189.44) because holdings re-marked: ANF×17 yday $145.75 → 09:30 $148.67 +49.64; SEDG×75 yday $33.51 → 09:30 $31.50 -150.75; SMTC×17 yday $142.43 → 09:30 $133.04 -159.63; URBN×31 yday $78.79 → 09:30 $81.09 +71.30 |
| 2026-09-01 | -6.30 | $161.21 | ANF×17, SEDG×75, SMTC×17, URBN×31 | $9,739.14 | -72.05 | — | — | $161.21 | $9,680.10 | ANF×17, SEDG×75, SMTC×17, URBN×31 | 09:30 open · cash $161.21 (unchanged overnight, no fees) · equity $9,739.14 vs prior close $9,811.19 (-72.05) because holdings re-marked: ANF×17 yday $149.28 → 09:30 $142.47 -115.77; SEDG×75 yday $31.27 → 09:30 $32.22 +71.25; SMTC×17 yday $132.54 → 09:30 $131.65 -15.13; URBN×31 yday $81.09 → 09:30 $80.69 -12.40 |
| 2026-09-02 | -3.83 | $161.21 | ANF×17, SEDG×75, SMTC×17, URBN×31 | $9,587.89 | -92.21 | — | ANF, SEDG, SMTC, URBN | $9,579.40 | $9,579.40 | — | 09:30 open · cash $161.21 (unchanged overnight, no fees) · equity $9,587.89 vs prior close $9,680.10 (-92.21) because holdings re-marked: ANF×17 yday $143.00 → 09:30 $142.00 -17.00; SEDG×75 yday $31.80 → 09:30 $31.87 +5.25; SMTC×17 yday $129.50 → 09:30 $127.63 -31.79; URBN×31 yday $80.69 → 09:30 $79.12 -48.67 |
| 2026-09-03 | -0.90 | $9,579.40 | — | $9,579.40 | -0.00 | RVTY, CRK, MMED, CTMX, CRDL, DEFT, MRNA, ARCT | — | $195.99 | $9,609.81 | RVTY×9, CRK×76, MMED×52, CTMX×321, CRDL×554, DEFT×1787, MRNA×7, ARCT×72 | 09:30 open · cash $9,579.40 · no holdings · equity $9,579.40 vs prior close $9,579.40 (-0.00). Cash unchanged overnight; no fees. |
| 2026-09-04 | — | $195.99 | RVTY×9, CRK×76, MMED×52, CTMX×321, CRDL×554, DEFT×1787, MRNA×7, ARCT×72 | $9,599.69 | -10.12 | CABA, GPRO, EOSE, CCOI, IRD, OABI | — | $64.18 | $9,493.13 | RVTY×9, CRK×76, MMED×52, CTMX×321, CRDL×554, DEFT×1787, MRNA×7, ARCT×72, CABA×6, GPRO×13, EOSE×6, CCOI×2, IRD×5, OABI×4 | 09:30 open · cash $195.99 (unchanged overnight, no fees) · equity $9,599.69 vs prior close $9,609.81 (-10.12) because holdings re-marked: RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×76 yday $15.54 → 09:30 $15.45 -6.84; MMED×52 yday $23.76 → 09:30 $23.88 +6.24; CTMX×321 yday $3.72 → 09:30 $3.73 +3.21; CRDL×554 yday $2.17 → 09:30 $2.18 +5.54; DEFT×1787 yday $0.65 → 09:30 $0.65 +0.00; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×72 yday $16.74 → 09:30 $16.77 +2.16 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | combo gate; gate vol=good,blue=True; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) because holdings re-marked: BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,628.11 vs yday $9,809.66 (-181.55) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,628.11 vs prior close $9,809.66 (-181.55) because holdings re-marked: BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,448.22 vs yday $9,454.54 (-6.32) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,448.22 vs prior close $9,454.54 (-6.32) because holdings re-marked: BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56 | — |
| 2026-08-19 09:30 ET | **SELL** | `BTBT` | 833 | $1.42 | $10.89 | $-88.28 | $1,182.24 | ▼ -88.28 after sell → book $9,437.32; vs 09:30 mark -10.90 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `BETR` | 84 | $13.03 | $2.27 | $-153.19 | $2,274.50 | ▼ -153.19 after sell → book $9,435.06; vs 09:30 mark -2.26 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ANGX` | 290 | $4.79 | $3.80 | $+131.66 | $3,659.80 | ▲ +131.66 after sell → book $9,431.26; vs 09:30 mark -3.80 | dropped from list after 3 sess (min 3) | join🔴 sector🟡 gen🔴 news🟢 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `HYLN` | 299 | $3.87 | $3.92 | $-100.46 | $4,813.01 | ▼ -100.46 after sell → book $9,427.34; vs 09:30 mark -3.92 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ADUR` | 75 | $15.65 | $2.24 | $-68.20 | $5,984.52 | ▼ -68.20 after sell → book $9,425.10; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `ARX` | 63 | $19.58 | $2.20 | $-3.75 | $7,215.86 | ▼ -3.75 after sell → book $9,422.90; vs 09:30 mark -2.20 | dropped from list after 3 sess (min 3) | join🟡 sector🟡 gen🔴 news🟡 vol🟢 buy🟡 |
| 2026-08-19 09:30 ET | **SELL** | `AIRO` | 112 | $9.10 | $2.35 | $-230.92 | $8,232.71 | ▼ -230.92 after sell → book $9,420.55; vs 09:30 mark -2.35 | dropped from list after 3 sess (min 3) | — |
| 2026-08-19 09:30 ET | **SELL** | `NCMI` | 464 | $2.56 | $6.07 | $-72.38 | $9,414.48 | ▼ -72.38 after sell → book $9,414.48; vs 09:30 mark -6.07 | dropped from list after 3 sess (min 3) | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,414.48 | ▲ 09:30 equity $9,414.48 vs yday $9,414.48 (-0.00) | 09:30 open · cash $9,414.48 · no holdings · equity $9,414.48 vs prior close $9,414.48 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-20 09:30 ET | **BUY** | `AG` | 57 | $20.55 | $2.16 | — | $8,240.97 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.9; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `BHP` | 12 | $91.01 | $2.03 | — | $7,146.82 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+2.4; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `CDE` | 56 | $20.65 | $2.16 | — | $5,988.26 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+11.3; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `HDSN` | 203 | $5.77 | $2.62 | — | $4,814.33 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+4.6; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `IAG` | 59 | $19.63 | $2.17 | — | $3,654.00 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.1; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `KGC` | 39 | $29.63 | $2.11 | — | $2,496.32 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+8.7; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `NFGC` | 672 | $1.75 | $8.67 | — | $1,311.65 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+7.9; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-20 09:30 ET | **BUY** | `WPM` | 8 | $144.54 | $2.01 | — | $153.32 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+9.2; leftover $1176.81 | join🟢 sector🟡 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $153.32 | ▲ 09:30 equity $9,863.41 vs yday $9,610.85 (+252.56) | 09:30 open · cash $153.32 (unchanged overnight, no fees) · equity $9,863.41 vs prior close $9,610.85 (+252.56) because holdings re-marked: AG×57 yday $21.19 → 09:30 $21.90 +40.47; BHP×12 yday $93.63 → 09:30 $95.72 +25.08; CDE×56 yday $21.11 → 09:30 $21.75 +35.84; HDSN×203 yday $5.57 → 09:30 $5.67 +20.30; IAG×59 yday $20.50 → 09:30 $21.17 +39.53; KGC×39 yday $31.43 → 09:30 $32.17 +28.86; NFGC×672 yday $1.75 → 09:30 $1.79 +26.88; WPM×8 yday $150.25 → 09:30 $154.70 +35.60 | — |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 1 | $17.20 | $0.17 | — | $135.94 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 1 | $11.13 | $0.11 | — | $124.70 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 7 | $2.47 | $0.19 | — | $107.21 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 9 | $1.93 | $0.20 | — | $89.64 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 14 | $1.32 | $0.23 | — | $70.94 | — | combo gate; gate vol=good,blue=True; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $19.16 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.94 | ▲ 09:30 equity $9,979.02 vs yday $9,861.85 (+117.17) | 09:30 open · cash $70.94 (unchanged overnight, no fees) · equity $9,979.02 vs prior close $9,861.85 (+117.17) because holdings re-marked: AG×57 yday $21.09 → 09:30 $21.47 +21.66; BHP×12 yday $97.03 → 09:30 $97.34 +3.72; CDE×56 yday $20.97 → 09:30 $21.26 +16.24; HDSN×203 yday $5.63 → 09:30 $5.69 +12.18; IAG×59 yday $21.14 → 09:30 $21.44 +17.70; KGC×39 yday $32.76 → 09:30 $33.21 +17.55; NFGC×672 yday $1.84 → 09:30 $1.86 +13.44; WPM×8 yday $157.78 → 09:30 $158.96 +9.44; AUPH×1 yday $16.65 → 09:30 $16.60 -0.05; ARCT×1 yday $13.45 → 09:30 $13.26 -0.19; AUTL×7 yday $2.41 → 09:30 $2.36 -0.35; CRDL×9 yday $1.86 → 09:30 $1.87 +0.09; CYPH×14 yday $1.42 → 09:30 $1.83 +5.74 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $70.94 | ▲ 09:30 equity $9,899.66 vs yday $9,835.05 (+64.61) | 09:30 open · cash $70.94 (unchanged overnight, no fees) · equity $9,899.66 vs prior close $9,835.05 (+64.61) because holdings re-marked: AG×57 yday $20.57 → 09:30 $20.73 +9.12; BHP×12 yday $96.66 → 09:30 $95.95 -8.52; CDE×56 yday $20.49 → 09:30 $20.85 +20.16; HDSN×203 yday $5.57 → 09:30 $5.53 -8.12; IAG×59 yday $21.36 → 09:30 $21.63 +15.93; KGC×39 yday $32.47 → 09:30 $32.76 +11.31; NFGC×672 yday $1.90 → 09:30 $1.91 +6.72; WPM×8 yday $158.00 → 09:30 $160.00 +16.00; AUPH×1 yday $16.60 → 09:30 $16.71 +0.11; ARCT×1 yday $13.76 → 09:30 $14.34 +0.58; AUTL×7 yday $2.38 → 09:30 $2.32 -0.42; CRDL×9 yday $1.80 → 09:30 $1.90 +0.90; CYPH×14 yday $1.64 → 09:30 $1.70 +0.84 | — |
| 2026-08-25 09:30 ET | **SELL** | `AG` | 57 | $20.73 | $2.18 | $+5.92 | $1,250.37 | ▲ +5.92 after sell → book $9,897.48; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `BHP` | 12 | $95.95 | $2.05 | $+55.21 | $2,399.72 | ▲ +55.21 after sell → book $9,895.43; vs 09:30 mark -2.05 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `CDE` | 56 | $20.85 | $2.18 | $+6.86 | $3,565.14 | ▲ +6.86 after sell → book $9,893.25; vs 09:30 mark -2.18 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `HDSN` | 203 | $5.53 | $2.66 | $-54.00 | $4,685.07 | ▼ -54.00 after sell → book $9,890.59; vs 09:30 mark -2.66 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `IAG` | 59 | $21.63 | $2.19 | $+113.65 | $5,959.05 | ▲ +113.65 after sell → book $9,888.40; vs 09:30 mark -2.19 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `KGC` | 39 | $32.76 | $2.13 | $+117.84 | $7,234.56 | ▲ +117.84 after sell → book $9,886.27; vs 09:30 mark -2.13 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `NFGC` | 672 | $1.91 | $8.79 | $+90.06 | $8,509.29 | ▲ +90.06 after sell → book $9,877.48; vs 09:30 mark -8.79 | dropped from list after 3 sess (min 3) | — |
| 2026-08-25 09:30 ET | **SELL** | `WPM` | 8 | $160.00 | $2.03 | $+119.63 | $9,787.26 | ▲ +119.63 after sell → book $9,875.45; vs 09:30 mark -2.03 | dropped from list after 3 sess (min 3) | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 863 | $1.62 | $11.13 | — | $8,378.07 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $1398.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 699 | $2.00 | $9.02 | — | $6,971.05 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $1398.18 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 377 | $3.70 | $4.86 | — | $5,571.29 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $1398.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 267 | $5.22 | $3.44 | — | $4,174.10 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $1398.18 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 205 | $6.79 | $2.64 | — | $2,779.51 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $1398.18 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 219 | $6.38 | $2.83 | — | $1,379.46 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $1398.18 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 156 | $8.79 | $2.46 | — | $5.76 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $1398.18 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.76 | ▲ 09:30 equity $10,000.04 vs yday $10,000.04 (+0.00) | 09:30 open · cash $5.76 (unchanged overnight, no fees) · equity $10,000.04 vs prior close $10,000.04 (+0.00) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.71 +0.00; ARCT×1 yday $14.21 → 09:30 $14.21 +0.00; AUTL×7 yday $2.34 → 09:30 $2.34 +0.00; CRDL×9 yday $1.90 → 09:30 $1.90 +0.00; CYPH×14 yday $1.64 → 09:30 $1.64 +0.00; BMEA×863 yday $1.61 → 09:30 $1.61 +0.00; NPWR×699 yday $2.02 → 09:30 $2.02 +0.00; PUSA×377 yday $3.91 → 09:30 $3.91 +0.00; ALVO×267 yday $5.25 → 09:30 $5.25 +0.00; CAPR×205 yday $7.19 → 09:30 $7.19 +0.00; ZURA×219 yday $6.50 → 09:30 $6.50 +0.00; SUJA×156 yday $8.54 → 09:30 $8.54 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $5.76 | ▲ 09:30 equity $10,238.67 vs yday $9,839.06 (+399.61) | 09:30 open · cash $5.76 (unchanged overnight, no fees) · equity $10,238.67 vs prior close $9,839.06 (+399.61) because holdings re-marked: AUPH×1 yday $16.71 → 09:30 $16.60 -0.11; ARCT×1 yday $14.21 → 09:30 $15.35 +1.14; AUTL×7 yday $2.34 → 09:30 $2.41 +0.49; CRDL×9 yday $1.90 → 09:30 $2.03 +1.17; CYPH×14 yday $1.64 → 09:30 $1.60 -0.56; BMEA×863 yday $1.61 → 09:30 $1.75 +120.82; NPWR×699 yday $2.02 → 09:30 $1.93 -62.91; PUSA×377 yday $3.91 → 09:30 $3.84 -26.39; ALVO×267 yday $5.25 → 09:30 $4.98 -72.09; CAPR×205 yday $7.19 → 09:30 $8.29 +225.50; ZURA×219 yday $6.50 → 09:30 $6.13 -81.03; SUJA×156 yday $8.54 → 09:30 $9.39 +132.60 | — |
| 2026-08-27 09:30 ET | **SELL** | `AUPH` | 1 | $16.60 | $0.19 | $-0.96 | $22.18 | ▼ -0.96 after sell → book $10,238.49; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `ARCT` | 1 | $15.35 | $0.18 | $+3.93 | $37.35 | ▲ +3.93 after sell → book $10,238.31; vs 09:30 mark -0.18 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `AUTL` | 7 | $2.41 | $0.21 | $-0.82 | $54.01 | ▼ -0.82 after sell → book $10,238.10; vs 09:30 mark -0.21 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CRDL` | 9 | $2.03 | $0.23 | $+0.47 | $72.05 | ▲ +0.47 after sell → book $10,237.87; vs 09:30 mark -0.23 | dropped from list after 4 sess (min 3) | — |
| 2026-08-27 09:30 ET | **SELL** | `CYPH` | 14 | $1.60 | $0.29 | $+3.41 | $94.16 | ▲ +3.41 after sell → book $10,237.58; vs 09:30 mark -0.29 | dropped from list after 4 sess (min 3) | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $94.16 | ▲ 09:30 equity $10,303.42 vs yday $10,300.75 (+2.67) | 09:30 open · cash $94.16 (unchanged overnight, no fees) · equity $10,303.42 vs prior close $10,300.75 (+2.67) because holdings re-marked: BMEA×863 yday $1.71 → 09:30 $1.74 +25.89; NPWR×699 yday $1.81 → 09:30 $1.83 +13.98; PUSA×377 yday $3.85 → 09:30 $3.86 +3.77; ALVO×267 yday $4.91 → 09:30 $4.88 -8.01; CAPR×205 yday $9.36 → 09:30 $9.19 -34.85; ZURA×219 yday $5.99 → 09:30 $6.02 +6.57; SUJA×156 yday $9.44 → 09:30 $9.41 -4.68 | — |
| 2026-08-28 09:30 ET | **SELL** | `BMEA` | 863 | $1.74 | $11.29 | $+81.14 | $1,584.50 | ▲ +81.14 after sell → book $10,292.14; vs 09:30 mark -11.28 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `NPWR` | 699 | $1.83 | $9.14 | $-136.99 | $2,854.52 | ▼ -136.99 after sell → book $10,282.99; vs 09:30 mark -9.15 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `PUSA` | 377 | $3.86 | $4.94 | $+50.52 | $4,304.81 | ▲ +50.52 after sell → book $10,278.06; vs 09:30 mark -4.93 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `ALVO` | 267 | $4.88 | $3.50 | $-97.72 | $5,604.27 | ▼ -97.72 after sell → book $10,274.56; vs 09:30 mark -3.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `CAPR` | 205 | $9.19 | $2.69 | $+486.66 | $7,485.52 | ▲ +486.66 after sell → book $10,271.86; vs 09:30 mark -2.70 | dropped from list after 3 sess (min 3) | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ZURA` | 219 | $6.02 | $2.87 | $-84.54 | $8,801.03 | ▼ -84.54 after sell → book $10,268.99; vs 09:30 mark -2.87 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **SELL** | `SUJA` | 156 | $9.41 | $2.50 | $+91.77 | $10,266.49 | ▲ +91.77 after sell → book $10,266.49; vs 09:30 mark -2.50 | dropped from list after 3 sess (min 3) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 17 | $144.70 | $2.04 | — | $7,804.55 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $2566.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 75 | $33.78 | $2.21 | — | $5,268.84 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $2566.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 17 | $149.40 | $2.04 | — | $2,727.00 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $2566.62 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 31 | $82.70 | $2.08 | — | $161.21 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $2566.62 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.21 | ▼ 09:30 equity $9,826.57 vs yday $10,016.01 (-189.44) | 09:30 open · cash $161.21 (unchanged overnight, no fees) · equity $9,826.57 vs prior close $10,016.01 (-189.44) because holdings re-marked: ANF×17 yday $145.75 → 09:30 $148.67 +49.64; SEDG×75 yday $33.51 → 09:30 $31.50 -150.75; SMTC×17 yday $142.43 → 09:30 $133.04 -159.63; URBN×31 yday $78.79 → 09:30 $81.09 +71.30 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.21 | ▼ 09:30 equity $9,739.14 vs yday $9,811.19 (-72.05) | 09:30 open · cash $161.21 (unchanged overnight, no fees) · equity $9,739.14 vs prior close $9,811.19 (-72.05) because holdings re-marked: ANF×17 yday $149.28 → 09:30 $142.47 -115.77; SEDG×75 yday $31.27 → 09:30 $32.22 +71.25; SMTC×17 yday $132.54 → 09:30 $131.65 -15.13; URBN×31 yday $81.09 → 09:30 $80.69 -12.40 | — |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $161.21 | ▼ 09:30 equity $9,587.89 vs yday $9,680.10 (-92.21) | 09:30 open · cash $161.21 (unchanged overnight, no fees) · equity $9,587.89 vs prior close $9,680.10 (-92.21) because holdings re-marked: ANF×17 yday $143.00 → 09:30 $142.00 -17.00; SEDG×75 yday $31.80 → 09:30 $31.87 +5.25; SMTC×17 yday $129.50 → 09:30 $127.63 -31.79; URBN×31 yday $80.69 → 09:30 $79.12 -48.67 | — |
| 2026-09-02 09:30 ET | **SELL** | `ANF` | 17 | $142.00 | $2.07 | $-50.01 | $2,573.14 | ▼ -50.01 after sell → book $9,585.82; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SEDG` | 75 | $31.87 | $2.25 | $-147.71 | $4,961.15 | ▼ -147.71 after sell → book $9,583.58; vs 09:30 mark -2.24 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `SMTC` | 17 | $127.63 | $2.07 | $-374.20 | $7,128.79 | ▼ -374.20 after sell → book $9,581.51; vs 09:30 mark -2.07 | dropped from list after 3 sess (min 3) | — |
| 2026-09-02 09:30 ET | **SELL** | `URBN` | 31 | $79.12 | $2.11 | $-115.18 | $9,579.40 | ▼ -115.18 after sell → book $9,579.40; vs 09:30 mark -2.11 | dropped from list after 3 sess (min 3) | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $9,579.40 | ▲ 09:30 equity $9,579.40 vs yday $9,579.40 (-0.00) | 09:30 open · cash $9,579.40 · no holdings · equity $9,579.40 vs prior close $9,579.40 (-0.00). Cash unchanged overnight; no fees. | — |
| 2026-09-03 09:30 ET | **BUY** | `RVTY` | 9 | $125.94 | $2.02 | — | $8,443.92 | — | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1197.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 76 | $15.70 | $2.22 | — | $7,248.50 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1197.42 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 52 | $22.78 | $2.15 | — | $6,061.80 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1197.42 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 321 | $3.72 | $4.14 | — | $4,863.54 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1197.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 554 | $2.16 | $7.15 | — | $3,659.75 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1197.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `DEFT` | 1787 | $0.67 | $17.33 | — | $2,445.12 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1197.42 | join🟡 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `MRNA` | 7 | $151.40 | $2.01 | — | $1,383.31 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1197.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🔴 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `ARCT` | 72 | $16.46 | $2.21 | — | $195.99 | — | combo gate; gate vol=good,blue=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1197.42 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $195.99 | ▼ 09:30 equity $9,599.69 vs yday $9,609.81 (-10.12) | 09:30 open · cash $195.99 (unchanged overnight, no fees) · equity $9,599.69 vs prior close $9,609.81 (-10.12) because holdings re-marked: RVTY×9 yday $130.94 → 09:30 $132.45 +13.59; CRK×76 yday $15.54 → 09:30 $15.45 -6.84; MMED×52 yday $23.76 → 09:30 $23.88 +6.24; CTMX×321 yday $3.72 → 09:30 $3.73 +3.21; CRDL×554 yday $2.17 → 09:30 $2.18 +5.54; DEFT×1787 yday $0.65 → 09:30 $0.65 +0.00; MRNA×7 yday $150.81 → 09:30 $145.95 -34.02; ARCT×72 yday $16.74 → 09:30 $16.77 +2.16 | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 6 | $3.63 | $0.24 | — | $173.97 | — | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $24.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `GPRO` | 13 | $1.78 | $0.27 | — | $150.56 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $24.50 | join🔴 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 6 | $3.57 | $0.23 | — | $128.91 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $24.50 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 2 | $10.22 | $0.21 | — | $108.26 | — | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $24.50 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `IRD` | 5 | $4.66 | $0.25 | — | $84.71 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $24.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `OABI` | 4 | $5.08 | $0.22 | — | $64.18 | — | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $24.50 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 1.28 < 1 share @ 4.05 |
| 2026-08-17 | `ABX` | cash | leftover split 1.28 < 1 share @ 9.12 |
| 2026-08-17 | `ALOY` | cash | leftover split 1.28 < 1 share @ 14.66 |
| 2026-08-17 | `NU` | cash | leftover split 1.28 < 1 share @ 15.40 |
| 2026-08-17 | `INV` | cash | leftover split 1.28 < 1 share @ 1.62 |
| 2026-08-17 | `KLC` | cash | leftover split 1.28 < 1 share @ 2.62 |
| 2026-08-17 | `ENHA` | cash | leftover split 1.28 < 1 share @ 2.01 |
| 2026-08-17 | `MP` | cash | leftover split 1.28 < 1 share @ 58.01 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ANGX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `UGI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DVLT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `NMAX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `TRGP` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-21 | `AG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `BHP` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `CDE` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `HDSN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `IAG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `KGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `NFGC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `WPM` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-21 | `AU` | cash | leftover split 19.16 < 1 share @ 119.43 |
| 2026-08-21 | `AEM` | cash | leftover split 19.16 < 1 share @ 216.30 |
| 2026-08-21 | `CRSP` | cash | leftover split 19.16 < 1 share @ 59.72 |
| 2026-08-24 | `AG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `BHP` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `CDE` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `HDSN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `IAG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `KGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `NFGC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `WPM` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
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
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-26 | `AUPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `ARCT` | no_price | no 09:30 open — carry |
| 2026-08-26 | `AUTL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CRDL` | no_price | no 09:30 open — carry |
| 2026-08-26 | `CYPH` | no_price | no 09:30 open — carry |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SRPT` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/3 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-04 | `RVTY` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DEFT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `MRNA` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `ARCT` | min_hold | dropped but min-hold 1/3 sess — no sell |
| 2026-09-04 | `DELL` | cash | leftover split 24.50 < 1 share @ 486.31 |
| 2026-09-04 | `MLYS` | cash | leftover split 24.50 < 1 share @ 29.15 |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `RVTY` | 9 | 2026-09-03 @ $125.94 | combo gate; gate vol=good,blue=True; list flatten,mover_buy; 🔵; ⚪; ret5=+6.8; leftover $1197.42 |
| `CRK` | 76 | 2026-09-03 @ $15.70 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $1197.42 |
| `MMED` | 52 | 2026-09-03 @ $22.78 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $1197.42 |
| `CTMX` | 321 | 2026-09-03 @ $3.72 | combo gate; gate vol=good,blue=True; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $1197.42 |
| `CRDL` | 554 | 2026-09-03 @ $2.16 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $1197.42 |
| `DEFT` | 1787 | 2026-09-03 @ $0.67 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover,ohlc_hot; 🔵; ret5=+17.6; leftover $1197.42 |
| `MRNA` | 7 | 2026-09-03 @ $151.40 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ret5=+129.2; leftover $1197.42 |
| `ARCT` | 72 | 2026-09-03 @ $16.46 | combo gate; gate vol=good,blue=True; list yday_gainer,mover_buy; 🔵; ⚪; ret5=+63.4; leftover $1197.42 |
| `CABA` | 6 | 2026-09-04 @ $3.63 | combo gate; gate vol=good,blue=True; list flatten; 🔵; ⚪; ret5=+13.8; leftover $24.50 |
| `GPRO` | 13 | 2026-09-04 @ $1.78 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=+5.9; leftover $24.50 |
| `EOSE` | 6 | 2026-09-04 @ $3.57 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $24.50 |
| `CCOI` | 2 | 2026-09-04 @ $10.22 | combo gate; gate vol=good,blue=True; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $24.50 |
| `IRD` | 5 | 2026-09-04 @ $4.66 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+12.2; leftover $24.50 |
| `OABI` | 4 | 2026-09-04 @ $5.08 | combo gate; gate vol=good,blue=True; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+28.1; leftover $24.50 |
