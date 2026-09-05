# Factor mine action — `union_vol_g_h5`

_Book rules: $10k · whole shares · Futubull fees · leftover cash split on new names · sell first · min-hold **5** sessions · fill 09:30 open · hard-red S≤-3 sit · shorts marked as liability (equity ≥ 2× notional). Live `flatten_robust` is not changed._

Research universe (not the live flatten gate). Cash/share/fee rules still apply.

Side **long** · universe `union` · top 8 · rank `list` · size `leftover` · sell `list` · S-boost `none` · union ∩ vol_g hold 5, no 🚨

Cash book **-10.12%** ($8,988) · signal-only (no cash/fees) was +7.06%. Starts YES **7/17**. Fills 73 · skips 177 · realized $-745.57.

## Why these stocks

Same shape as [FLATTEN_LOOKBACK_ACTION.md](../FLATTEN_LOOKBACK_ACTION.md): the 09:30 packet + leftover cash + lots on hand decide the ticket. Same-day Change% is outcome only.

- **Universe** `union` — candidate list at 09:30 (flatten wish-list, union, probable, yday gainer, or OHLC hot).
- **Gate** `vol=good` · **rank** `list order` · **top_n** 8.
- **Size** `leftover` splits leftover cash among *new* names only. Rank-weight / top-heavy still cannot invent money.
- **Sell** `list` after min-hold **5**. We never sell a ticker we do not hold. Early 🚨 / last-red / news🔴 can still exit inside the floor.
- **Entry:** Research universe (not the live flatten gate). Cash/share/fee rules still apply.

## State audit

**PASS** · 0 violations. Independent replay of fills never sold an unheld lot and never spent past leftover cash. Close cash $326.26.

## Each session (cash + holdings state)

| Date | S | 09:30 cash | 09:30 held | 09:30 equity | vs yday close | Bought | Sold | Close cash | Close equity | Close held | 09:30 why |
|---|---:|---:|---|---:|---:|---|---|---:|---:|---|---|
| 2026-08-13 | +8.53 | $10,000.00 | — | $10,000.00 | +0.00 | — | — | $10,000.00 | $10,000.00 | — | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-14 | +5.50 | $10,000.00 | — | $10,000.00 | +0.00 | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | — | $10.28 | $9,797.82 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. |
| 2026-08-17 | +2.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,768.32 | -29.50 | — | — | $10.28 | $9,809.66 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) because holdings re-marked: BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 |
| 2026-08-18 | -6.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,628.11 | -181.55 | — | — | $10.28 | $9,454.54 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,628.11 vs prior close $9,809.66 (-181.55) because holdings re-marked: BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28 |
| 2026-08-19 | -7.20 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,448.22 | -6.32 | — | — | $10.28 | $9,275.27 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,448.22 vs prior close $9,454.54 (-6.32) because holdings re-marked: BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56 |
| 2026-08-20 | +1.12 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,267.56 | -7.71 | — | — | $10.28 | $9,115.98 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,267.56 vs prior close $9,275.27 (-7.71) because holdings re-marked: BTBT×833 yday $1.40 → 09:30 $1.46 +45.82; BETR×84 yday $13.03 → 09:30 $12.95 -6.72; ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; HYLN×299 yday $3.67 → 09:30 $3.61 -17.94; ADUR×75 yday $15.39 → 09:30 $15.55 +12.00; ARX×63 yday $19.55 → 09:30 $19.55 +0.00; AIRO×112 yday $8.59 → 09:30 $8.51 -8.96; NCMI×464 yday $2.64 → 09:30 $2.59 -23.20 |
| 2026-08-21 | +3.25 | $10.28 | BTBT×833, BETR×84, ANGX×290, HYLN×299, ADUR×75, ARX×63, AIRO×112, NCMI×464 | $9,241.45 | +125.47 | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | BTBT, BETR, ANGX, HYLN, ADUR, ARX, AIRO, NCMI | $151.04 | $9,403.10 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871 | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,241.45 vs prior close $9,115.98 (+125.47) because holdings re-marked: BTBT×833 yday $1.59 → 09:30 $1.66 +54.14; BETR×84 yday $11.60 → 09:30 $11.73 +10.92; ANGX×290 yday $4.37 → 09:30 $4.43 +17.40; HYLN×299 yday $3.37 → 09:30 $3.42 +14.95; ADUR×75 yday $15.85 → 09:30 $16.00 +11.25; ARX×63 yday $19.57 → 09:30 $19.57 +0.00; AIRO×112 yday $8.24 → 09:30 $8.39 +16.80; NCMI×464 yday $2.55 → 09:30 $2.55 +0.00 |
| 2026-08-24 | -5.17 | $151.04 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871 | $9,704.93 | +301.83 | — | — | $151.04 | $9,491.49 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871 | 09:30 open · cash $151.04 (unchanged overnight, no fees) · equity $9,704.93 vs prior close $9,403.10 (+301.83) because holdings re-marked: AU×9 yday $121.22 → 09:30 $120.50 -6.48; AUPH×66 yday $16.65 → 09:30 $16.60 -3.30; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×103 yday $13.45 → 09:30 $13.26 -19.57; AUTL×465 yday $2.41 → 09:30 $2.36 -23.25; CRDL×596 yday $1.86 → 09:30 $1.87 +5.96; CRSP×19 yday $59.50 → 09:30 $58.79 -13.49; CYPH×871 yday $1.42 → 09:30 $1.83 +357.11 |
| 2026-08-25 | +1.80 | $151.04 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871 | $9,583.36 | +91.87 | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA | — | $12.01 | $9,594.79 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | 09:30 open · cash $151.04 (unchanged overnight, no fees) · equity $9,583.36 vs prior close $9,491.49 (+91.87) because holdings re-marked: AU×9 yday $118.66 → 09:30 $119.46 +7.20; AUPH×66 yday $16.60 → 09:30 $16.71 +7.26; AEM×5 yday $214.08 → 09:30 $200.48 -68.00; ARCT×103 yday $13.76 → 09:30 $14.34 +59.74; AUTL×465 yday $2.38 → 09:30 $2.32 -27.90; CRDL×596 yday $1.80 → 09:30 $1.90 +59.60; CRSP×19 yday $56.91 → 09:30 $57.00 +1.71; CYPH×871 yday $1.64 → 09:30 $1.70 +52.26 |
| 2026-08-26 | +2.02 | $12.01 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | $9,594.79 | +0.00 | — | — | $12.01 | $9,581.86 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | 09:30 open · cash $12.01 (unchanged overnight, no fees) · equity $9,594.79 vs prior close $9,594.79 (+0.00) because holdings re-marked: AU×9 yday $118.55 → 09:30 $118.55 +0.00; AUPH×66 yday $16.71 → 09:30 $16.71 +0.00; AEM×5 yday $215.40 → 09:30 $215.40 +0.00; ARCT×103 yday $14.21 → 09:30 $14.21 +0.00; AUTL×465 yday $2.34 → 09:30 $2.34 +0.00; CRDL×596 yday $1.90 → 09:30 $1.90 +0.00; CRSP×19 yday $57.03 → 09:30 $57.03 +0.00; CYPH×871 yday $1.64 → 09:30 $1.64 +0.00; BMEA×13 yday $1.61 → 09:30 $1.61 +0.00; NPWR×10 yday $2.02 → 09:30 $2.02 +0.00; PUSA×5 yday $3.91 → 09:30 $3.91 +0.00; ALVO×4 yday $5.25 → 09:30 $5.25 +0.00; CAPR×3 yday $7.19 → 09:30 $7.19 +0.00; ZURA×3 yday $6.50 → 09:30 $6.50 +0.00; SUJA×2 yday $8.54 → 09:30 $8.54 +0.00 |
| 2026-08-27 | — | $12.01 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | $9,875.12 | +293.26 | — | — | $12.01 | $9,915.47 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | 09:30 open · cash $12.01 (unchanged overnight, no fees) · equity $9,875.12 vs prior close $9,581.86 (+293.26) because holdings re-marked: AU×9 yday $118.55 → 09:30 $119.80 +11.25; AUPH×66 yday $16.71 → 09:30 $16.60 -7.26; AEM×5 yday $215.40 → 09:30 $219.50 +20.50; ARCT×103 yday $14.21 → 09:30 $15.35 +117.42; AUTL×465 yday $2.34 → 09:30 $2.41 +32.55; CRDL×596 yday $1.90 → 09:30 $2.03 +77.48; CRSP×19 yday $57.03 → 09:30 $60.18 +59.85; CYPH×871 yday $1.64 → 09:30 $1.60 -34.84; BMEA×13 yday $1.61 → 09:30 $1.75 +1.82; NPWR×10 yday $2.02 → 09:30 $1.93 -0.90; PUSA×5 yday $3.91 → 09:30 $3.84 -0.35; ALVO×4 yday $5.25 → 09:30 $4.98 -1.08; CAPR×3 yday $7.19 → 09:30 $8.29 +3.30; ZURA×3 yday $6.50 → 09:30 $6.13 -1.11; SUJA×2 yday $8.54 → 09:30 $9.39 +1.70 |
| 2026-08-28 | +0.75 | $12.01 | AU×9, AUPH×66, AEM×5, ARCT×103, AUTL×465, CRDL×596, CRSP×19, CYPH×871, BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2 | $9,963.65 | +48.18 | ANF, BHVN, BZ, SEDG, SMTC, URBN, ERAS | AU, AUPH, AEM, ARCT, AUTL, CRDL, CRSP, CYPH | $246.60 | $9,694.83 | BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2, ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | 09:30 open · cash $12.01 (unchanged overnight, no fees) · equity $9,963.65 vs prior close $9,915.47 (+48.18) because holdings re-marked: AU×9 yday $118.11 → 09:30 $117.41 -6.30; AUPH×66 yday $16.54 → 09:30 $16.47 -4.62; AEM×5 yday $214.04 → 09:30 $214.11 +0.35; ARCT×103 yday $15.83 → 09:30 $15.74 -9.27; AUTL×465 yday $2.33 → 09:30 $2.32 -4.65; CRDL×596 yday $2.14 → 09:30 $2.09 -29.80; CRSP×19 yday $59.23 → 09:30 $59.12 -2.09; CYPH×871 yday $1.63 → 09:30 $1.75 +104.52; BMEA×13 yday $1.71 → 09:30 $1.74 +0.39; NPWR×10 yday $1.81 → 09:30 $1.83 +0.20; PUSA×5 yday $3.85 → 09:30 $3.86 +0.05; ALVO×4 yday $4.91 → 09:30 $4.88 -0.12; CAPR×3 yday $9.36 → 09:30 $9.19 -0.51; ZURA×3 yday $5.99 → 09:30 $6.02 +0.09; SUJA×2 yday $9.44 → 09:30 $9.41 -0.06 |
| 2026-08-31 | -5.85 | $246.60 | BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2, ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | $9,411.64 | -283.19 | — | — | $246.60 | $9,401.04 | BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2, ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | 09:30 open · cash $246.60 (unchanged overnight, no fees) · equity $9,411.64 vs prior close $9,694.83 (-283.19) because holdings re-marked: BMEA×13 yday $1.68 → 09:30 $1.71 +0.39; NPWR×10 yday $1.89 → 09:30 $1.83 -0.60; PUSA×5 yday $3.79 → 09:30 $3.72 -0.35; ALVO×4 yday $4.88 → 09:30 $4.98 +0.40; CAPR×3 yday $10.06 → 09:30 $9.44 -1.86; ZURA×3 yday $5.85 → 09:30 $5.51 -1.02; SUJA×2 yday $9.00 → 09:30 $10.09 +2.18; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×82 yday $16.12 → 09:30 $15.44 -55.76; BZ×75 yday $18.00 → 09:30 $17.89 -8.25; SEDG×41 yday $33.51 → 09:30 $31.50 -82.41; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; ERAS×72 yday $19.49 → 09:30 $17.90 -114.48 |
| 2026-09-01 | -6.30 | $246.60 | BMEA×13, NPWR×10, PUSA×5, ALVO×4, CAPR×3, ZURA×3, SUJA×2, ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | $9,337.96 | -63.08 | — | BMEA, NPWR, PUSA, ALVO, CAPR, ZURA, SUJA | $391.44 | $9,267.83 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | 09:30 open · cash $246.60 (unchanged overnight, no fees) · equity $9,337.96 vs prior close $9,401.04 (-63.08) because holdings re-marked: BMEA×13 yday $1.71 → 09:30 $1.65 -0.78; NPWR×10 yday $1.82 → 09:30 $1.78 -0.40; PUSA×5 yday $3.80 → 09:30 $3.93 +0.65; ALVO×4 yday $4.96 → 09:30 $5.24 +1.12; CAPR×3 yday $9.36 → 09:30 $10.43 +3.21; ZURA×3 yday $5.64 → 09:30 $5.60 -0.12; SUJA×2 yday $10.09 → 09:30 $9.31 -1.56; ANF×9 yday $149.28 → 09:30 $142.47 -61.29; BHVN×82 yday $15.40 → 09:30 $15.45 +4.10; BZ×75 yday $17.90 → 09:30 $17.37 -39.75; SEDG×41 yday $31.27 → 09:30 $32.22 +38.95; SMTC×9 yday $132.54 → 09:30 $131.65 -8.01; URBN×16 yday $81.09 → 09:30 $80.69 -6.40; ERAS×72 yday $17.90 → 09:30 $18.00 +7.20 |
| 2026-09-02 | -3.83 | $391.44 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | $9,215.19 | -52.64 | — | — | $391.44 | $9,262.37 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | 09:30 open · cash $391.44 (unchanged overnight, no fees) · equity $9,215.19 vs prior close $9,267.83 (-52.64) because holdings re-marked: ANF×9 yday $143.00 → 09:30 $142.00 -9.00; BHVN×82 yday $15.45 → 09:30 $15.39 -4.92; BZ×75 yday $17.17 → 09:30 $17.29 +9.00; SEDG×41 yday $31.80 → 09:30 $31.87 +2.87; SMTC×9 yday $129.50 → 09:30 $127.63 -16.83; URBN×16 yday $80.69 → 09:30 $79.12 -25.12; ERAS×72 yday $17.70 → 09:30 $17.58 -8.64 |
| 2026-09-03 | -0.90 | $391.44 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72 | $9,291.08 | +28.71 | GPRO, FRVO, CRK, MMED, CTMX, CRDL | — | $114.32 | $9,258.07 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72, GPRO×40, FRVO×2, CRK×3, MMED×2, CTMX×13, CRDL×22 | 09:30 open · cash $391.44 (unchanged overnight, no fees) · equity $9,291.08 vs prior close $9,262.37 (+28.71) because holdings re-marked: ANF×9 yday $140.68 → 09:30 $139.65 -9.27; BHVN×82 yday $15.74 → 09:30 $15.97 +18.86; BZ×75 yday $17.55 → 09:30 $17.65 +7.50; SEDG×41 yday $32.49 → 09:30 $32.42 -2.87; SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; URBN×16 yday $79.29 → 09:30 $78.84 -7.20; ERAS×72 yday $16.76 → 09:30 $16.97 +15.12 |
| 2026-09-04 | — | $114.32 | ANF×9, BHVN×82, BZ×75, SEDG×41, SMTC×9, URBN×16, ERAS×72, GPRO×40, FRVO×2, CRK×3, MMED×2, CTMX×13, CRDL×22 | $9,290.58 | +32.51 | CABA, BAK, EOSE, DELL, MLYS, CCOI, SGLD | ANF, BHVN, BZ, SEDG, SMTC, URBN, ERAS | $326.26 | $8,988.29 | GPRO×40, FRVO×2, CRK×3, MMED×2, CTMX×13, CRDL×22, CABA×353, BAK×657, EOSE×359, DELL×2, MLYS×43, CCOI×125, SGLD×197 | 09:30 open · cash $114.32 (unchanged overnight, no fees) · equity $9,290.58 vs prior close $9,258.07 (+32.51) because holdings re-marked: ANF×9 yday $136.60 → 09:30 $137.70 +9.90; BHVN×82 yday $15.69 → 09:30 $15.89 +16.40; BZ×75 yday $17.30 → 09:30 $17.31 +0.75; SEDG×41 yday $33.98 → 09:30 $33.69 -11.89; SMTC×9 yday $133.85 → 09:30 $133.10 -6.75; URBN×16 yday $78.75 → 09:30 $79.93 +18.88; ERAS×72 yday $16.37 → 09:30 $16.38 +0.72; GPRO×40 yday $1.69 → 09:30 $1.78 +3.60; FRVO×2 yday $17.98 → 09:30 $18.27 +0.58; CRK×3 yday $15.54 → 09:30 $15.45 -0.27; MMED×2 yday $23.76 → 09:30 $23.88 +0.24; CTMX×13 yday $3.72 → 09:30 $3.73 +0.13; CRDL×22 yday $2.17 → 09:30 $2.18 +0.22 |

## Fills (09:30 open snapshot, then buys / sells)

| Date 09:30 ET | Side | Ticker | Shares | Px | Fees | P/L | Cash after | Equity change (sells only) | Why | Cameras |
|---|---|---|---:|---:|---:|---:|---:|---:|---|---|
| 2026-08-13 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10,000.00 | ▲ 09:30 equity $10,000.00 vs yday $10,000.00 (+0.00) | 09:30 open · cash $10,000.00 · no holdings · equity $10,000.00 vs prior close $10,000.00 (+0.00). Cash unchanged overnight; no fees. | — |
| 2026-08-14 09:30 ET | **BUY** | `BTBT` | 833 | $1.50 | $10.75 | — | $8,739.75 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+9.2; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `BETR` | 84 | $14.80 | $2.24 | — | $7,494.31 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=-9.9; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ANGX` | 290 | $4.31 | $3.74 | — | $6,240.67 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `HYLN` | 299 | $4.18 | $3.86 | — | $4,986.99 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+4.1; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ADUR` | 75 | $16.50 | $2.21 | — | $3,747.28 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,ohlc_hot; 🔵; ⚪; ret5=+9.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `ARX` | 63 | $19.57 | $2.18 | — | $2,512.19 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+58.7; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟢 judge🟢 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `AIRO` | 112 | $11.12 | $2.33 | — | $1,264.42 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+36.3; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-14 09:30 ET | **BUY** | `NCMI` | 464 | $2.69 | $5.99 | — | $10.28 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=-33.5; leftover $1250.00 | join🟢 sector🟢 gen🟢 news🟡 vol🟢 buy🟡 |
| 2026-08-17 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,768.32 vs yday $9,797.82 (-29.50) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,768.32 vs prior close $9,797.82 (-29.50) because holdings re-marked: BTBT×833 yday $1.57 → 09:30 $1.52 -41.65; BETR×84 yday $13.73 → 09:30 $13.67 -5.04; ANGX×290 yday $4.37 → 09:30 $4.60 +66.70; HYLN×299 yday $4.06 → 09:30 $4.10 +11.96; ADUR×75 yday $16.17 → 09:30 $15.73 -33.00; ARX×63 yday $19.58 → 09:30 $19.57 -0.63; AIRO×112 yday $9.57 → 09:30 $9.57 +0.00; NCMI×464 yday $2.86 → 09:30 $2.80 -27.84 | — |
| 2026-08-18 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,628.11 vs yday $9,809.66 (-181.55) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,628.11 vs prior close $9,809.66 (-181.55) because holdings re-marked: BTBT×833 yday $1.60 → 09:30 $1.54 -49.98; BETR×84 yday $13.54 → 09:30 $13.21 -27.72; ANGX×290 yday $4.71 → 09:30 $4.79 +23.20; HYLN×299 yday $4.09 → 09:30 $3.95 -41.86; ADUR×75 yday $15.85 → 09:30 $15.41 -33.00; ARX×63 yday $19.54 → 09:30 $19.57 +1.89; AIRO×112 yday $9.41 → 09:30 $9.01 -44.80; NCMI×464 yday $2.73 → 09:30 $2.71 -9.28 | — |
| 2026-08-19 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,448.22 vs yday $9,454.54 (-6.32) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,448.22 vs prior close $9,454.54 (-6.32) because holdings re-marked: BTBT×833 yday $1.45 → 09:30 $1.42 -24.99; BETR×84 yday $13.05 → 09:30 $13.03 -1.68; ANGX×290 yday $4.85 → 09:30 $4.79 -17.40; HYLN×299 yday $3.86 → 09:30 $3.87 +2.99; ADUR×75 yday $15.63 → 09:30 $15.65 +1.50; ARX×63 yday $19.56 → 09:30 $19.58 +1.26; AIRO×112 yday $8.98 → 09:30 $9.10 +13.44; NCMI×464 yday $2.52 → 09:30 $2.56 +18.56 | — |
| 2026-08-20 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▼ 09:30 equity $9,267.56 vs yday $9,275.27 (-7.71) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,267.56 vs prior close $9,275.27 (-7.71) because holdings re-marked: BTBT×833 yday $1.40 → 09:30 $1.46 +45.82; BETR×84 yday $13.03 → 09:30 $12.95 -6.72; ANGX×290 yday $4.60 → 09:30 $4.57 -8.70; HYLN×299 yday $3.67 → 09:30 $3.61 -17.94; ADUR×75 yday $15.39 → 09:30 $15.55 +12.00; ARX×63 yday $19.55 → 09:30 $19.55 +0.00; AIRO×112 yday $8.59 → 09:30 $8.51 -8.96; NCMI×464 yday $2.64 → 09:30 $2.59 -23.20 | — |
| 2026-08-21 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $10.28 | ▲ 09:30 equity $9,241.45 vs yday $9,115.98 (+125.47) | 09:30 open · cash $10.28 (unchanged overnight, no fees) · equity $9,241.45 vs prior close $9,115.98 (+125.47) because holdings re-marked: BTBT×833 yday $1.59 → 09:30 $1.66 +54.14; BETR×84 yday $11.60 → 09:30 $11.73 +10.92; ANGX×290 yday $4.37 → 09:30 $4.43 +17.40; HYLN×299 yday $3.37 → 09:30 $3.42 +14.95; ADUR×75 yday $15.85 → 09:30 $16.00 +11.25; ARX×63 yday $19.57 → 09:30 $19.57 +0.00; AIRO×112 yday $8.24 → 09:30 $8.39 +16.80; NCMI×464 yday $2.55 → 09:30 $2.55 +0.00 | — |
| 2026-08-21 09:30 ET | **SELL** | `BTBT` | 833 | $1.66 | $10.89 | $+111.64 | $1,382.16 | ▲ +111.64 after sell → book $9,230.55; vs 09:30 mark -10.90 | dropped from list after 5 sess (min 5) | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **SELL** | `BETR` | 84 | $11.73 | $2.27 | $-262.39 | $2,365.22 | ▼ -262.39 after sell → book $9,228.29; vs 09:30 mark -2.26 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ANGX` | 290 | $4.43 | $3.80 | $+27.26 | $3,646.12 | ▲ +27.26 after sell → book $9,224.49; vs 09:30 mark -3.80 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `HYLN` | 299 | $3.42 | $3.92 | $-235.01 | $4,664.78 | ▼ -235.01 after sell → book $9,220.57; vs 09:30 mark -3.92 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ADUR` | 75 | $16.00 | $2.24 | $-41.95 | $5,862.54 | ▼ -41.95 after sell → book $9,218.33; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `ARX` | 63 | $19.57 | $2.20 | $-4.38 | $7,093.25 | ▼ -4.38 after sell → book $9,216.13; vs 09:30 mark -2.20 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `AIRO` | 112 | $8.39 | $2.35 | $-310.44 | $8,030.58 | ▼ -310.44 after sell → book $9,213.78; vs 09:30 mark -2.35 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **SELL** | `NCMI` | 464 | $2.55 | $6.07 | $-77.02 | $9,207.71 | ▼ -77.02 after sell → book $9,207.71; vs 09:30 mark -6.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-21 09:30 ET | **BUY** | `AU` | 9 | $119.43 | $2.02 | — | $8,130.82 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+20.4; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUPH` | 66 | $17.20 | $2.19 | — | $6,993.43 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+13.8; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟢 |
| 2026-08-21 09:30 ET | **BUY** | `AEM` | 5 | $216.30 | $2.00 | — | $5,909.93 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,ohlc_hot,mover_buy; 🔵; ⚪; ret5=+17.6; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `ARCT` | 103 | $11.13 | $2.30 | — | $4,761.24 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+39.8; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `AUTL` | 465 | $2.47 | $6.00 | — | $3,606.69 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.8; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRDL` | 596 | $1.93 | $7.69 | — | $2,448.72 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+10.2; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CRSP` | 19 | $59.72 | $2.05 | — | $1,311.99 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,mover_buy; 🔵; ⚪; ret5=+7.6; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-21 09:30 ET | **BUY** | `CYPH` | 871 | $1.32 | $11.24 | — | $151.04 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten,yday_gainer,mover_buy; 🔵; ⚪; ret5=+83.6; leftover $1150.96 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-24 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.04 | ▲ 09:30 equity $9,704.93 vs yday $9,403.10 (+301.83) | 09:30 open · cash $151.04 (unchanged overnight, no fees) · equity $9,704.93 vs prior close $9,403.10 (+301.83) because holdings re-marked: AU×9 yday $121.22 → 09:30 $120.50 -6.48; AUPH×66 yday $16.65 → 09:30 $16.60 -3.30; AEM×5 yday $216.06 → 09:30 $217.03 +4.85; ARCT×103 yday $13.45 → 09:30 $13.26 -19.57; AUTL×465 yday $2.41 → 09:30 $2.36 -23.25; CRDL×596 yday $1.86 → 09:30 $1.87 +5.96; CRSP×19 yday $59.50 → 09:30 $58.79 -13.49; CYPH×871 yday $1.42 → 09:30 $1.83 +357.11 | — |
| 2026-08-25 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $151.04 | ▲ 09:30 equity $9,583.36 vs yday $9,491.49 (+91.87) | 09:30 open · cash $151.04 (unchanged overnight, no fees) · equity $9,583.36 vs prior close $9,491.49 (+91.87) because holdings re-marked: AU×9 yday $118.66 → 09:30 $119.46 +7.20; AUPH×66 yday $16.60 → 09:30 $16.71 +7.26; AEM×5 yday $214.08 → 09:30 $200.48 -68.00; ARCT×103 yday $13.76 → 09:30 $14.34 +59.74; AUTL×465 yday $2.38 → 09:30 $2.32 -27.90; CRDL×596 yday $1.80 → 09:30 $1.90 +59.60; CRSP×19 yday $56.91 → 09:30 $57.00 +1.71; CYPH×871 yday $1.64 → 09:30 $1.70 +52.26 | — |
| 2026-08-25 09:30 ET | **BUY** | `BMEA` | 13 | $1.62 | $0.25 | — | $129.73 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=+3.8; leftover $21.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `NPWR` | 10 | $2.00 | $0.23 | — | $109.50 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-12.8; leftover $21.58 | join🟡 sector🟢 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `PUSA` | 5 | $3.70 | $0.20 | — | $90.80 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.8; leftover $21.58 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ALVO` | 4 | $5.22 | $0.22 | — | $69.70 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+9.2; leftover $21.58 | join🟡 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `CAPR` | 3 | $6.79 | $0.21 | — | $49.12 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.4; leftover $21.58 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 peer🔴 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `ZURA` | 3 | $6.38 | $0.20 | — | $29.78 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ⚪; ret5=+5.0; leftover $21.58 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 vol🟢 buy🟡 |
| 2026-08-25 09:30 ET | **BUY** | `SUJA` | 2 | $8.79 | $0.18 | — | $12.01 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ⚪; ret5=+18.5; leftover $21.58 | join🟡 sector🟡 gen🟡 news🟡 digest🟡 ab🟡 vol🟢 buy🟡 |
| 2026-08-26 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.01 | ▲ 09:30 equity $9,594.79 vs yday $9,594.79 (+0.00) | 09:30 open · cash $12.01 (unchanged overnight, no fees) · equity $9,594.79 vs prior close $9,594.79 (+0.00) because holdings re-marked: AU×9 yday $118.55 → 09:30 $118.55 +0.00; AUPH×66 yday $16.71 → 09:30 $16.71 +0.00; AEM×5 yday $215.40 → 09:30 $215.40 +0.00; ARCT×103 yday $14.21 → 09:30 $14.21 +0.00; AUTL×465 yday $2.34 → 09:30 $2.34 +0.00; CRDL×596 yday $1.90 → 09:30 $1.90 +0.00; CRSP×19 yday $57.03 → 09:30 $57.03 +0.00; CYPH×871 yday $1.64 → 09:30 $1.64 +0.00; BMEA×13 yday $1.61 → 09:30 $1.61 +0.00; NPWR×10 yday $2.02 → 09:30 $2.02 +0.00; PUSA×5 yday $3.91 → 09:30 $3.91 +0.00; ALVO×4 yday $5.25 → 09:30 $5.25 +0.00; CAPR×3 yday $7.19 → 09:30 $7.19 +0.00; ZURA×3 yday $6.50 → 09:30 $6.50 +0.00; SUJA×2 yday $8.54 → 09:30 $8.54 +0.00 | — |
| 2026-08-27 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.01 | ▲ 09:30 equity $9,875.12 vs yday $9,581.86 (+293.26) | 09:30 open · cash $12.01 (unchanged overnight, no fees) · equity $9,875.12 vs prior close $9,581.86 (+293.26) because holdings re-marked: AU×9 yday $118.55 → 09:30 $119.80 +11.25; AUPH×66 yday $16.71 → 09:30 $16.60 -7.26; AEM×5 yday $215.40 → 09:30 $219.50 +20.50; ARCT×103 yday $14.21 → 09:30 $15.35 +117.42; AUTL×465 yday $2.34 → 09:30 $2.41 +32.55; CRDL×596 yday $1.90 → 09:30 $2.03 +77.48; CRSP×19 yday $57.03 → 09:30 $60.18 +59.85; CYPH×871 yday $1.64 → 09:30 $1.60 -34.84; BMEA×13 yday $1.61 → 09:30 $1.75 +1.82; NPWR×10 yday $2.02 → 09:30 $1.93 -0.90; PUSA×5 yday $3.91 → 09:30 $3.84 -0.35; ALVO×4 yday $5.25 → 09:30 $4.98 -1.08; CAPR×3 yday $7.19 → 09:30 $8.29 +3.30; ZURA×3 yday $6.50 → 09:30 $6.13 -1.11; SUJA×2 yday $8.54 → 09:30 $9.39 +1.70 | — |
| 2026-08-28 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $12.01 | ▲ 09:30 equity $9,963.65 vs yday $9,915.47 (+48.18) | 09:30 open · cash $12.01 (unchanged overnight, no fees) · equity $9,963.65 vs prior close $9,915.47 (+48.18) because holdings re-marked: AU×9 yday $118.11 → 09:30 $117.41 -6.30; AUPH×66 yday $16.54 → 09:30 $16.47 -4.62; AEM×5 yday $214.04 → 09:30 $214.11 +0.35; ARCT×103 yday $15.83 → 09:30 $15.74 -9.27; AUTL×465 yday $2.33 → 09:30 $2.32 -4.65; CRDL×596 yday $2.14 → 09:30 $2.09 -29.80; CRSP×19 yday $59.23 → 09:30 $59.12 -2.09; CYPH×871 yday $1.63 → 09:30 $1.75 +104.52; BMEA×13 yday $1.71 → 09:30 $1.74 +0.39; NPWR×10 yday $1.81 → 09:30 $1.83 +0.20; PUSA×5 yday $3.85 → 09:30 $3.86 +0.05; ALVO×4 yday $4.91 → 09:30 $4.88 -0.12; CAPR×3 yday $9.36 → 09:30 $9.19 -0.51; ZURA×3 yday $5.99 → 09:30 $6.02 +0.09; SUJA×2 yday $9.44 → 09:30 $9.41 -0.06 | — |
| 2026-08-28 09:30 ET | **SELL** | `AU` | 9 | $117.41 | $2.04 | $-22.23 | $1,066.67 | ▼ -22.23 after sell → book $9,961.62; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUPH` | 66 | $16.47 | $2.21 | $-52.58 | $2,151.48 | ▼ -52.58 after sell → book $9,959.41; vs 09:30 mark -2.21 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AEM` | 5 | $214.11 | $2.02 | $-14.98 | $3,220.00 | ▼ -14.98 after sell → book $9,957.38; vs 09:30 mark -2.03 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🔴 heat🟡 vol🟡 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `ARCT` | 103 | $15.74 | $2.33 | $+470.20 | $4,838.89 | ▲ +470.20 after sell → book $9,955.05; vs 09:30 mark -2.33 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `AUTL` | 465 | $2.32 | $6.09 | $-81.83 | $5,911.61 | ▼ -81.83 after sell → book $9,948.97; vs 09:30 mark -6.08 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CRDL` | 596 | $2.09 | $7.80 | $+79.87 | $7,149.45 | ▲ +79.87 after sell → book $9,941.17; vs 09:30 mark -7.80 | dropped from list after 5 sess (min 5) | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **SELL** | `CRSP` | 19 | $59.12 | $2.07 | $-15.51 | $8,270.66 | ▼ -15.51 after sell → book $9,939.10; vs 09:30 mark -2.07 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **SELL** | `CYPH` | 871 | $1.75 | $11.39 | $+351.90 | $9,783.52 | ▲ +351.90 after sell → book $9,927.71; vs 09:30 mark -11.39 | dropped from list after 5 sess (min 5) | — |
| 2026-08-28 09:30 ET | **BUY** | `ANF` | 9 | $144.70 | $2.02 | — | $8,479.20 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=+0.8; leftover $1397.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BHVN` | 82 | $16.95 | $2.24 | — | $7,087.07 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-5.4; leftover $1397.65 | join🔴 sector🔴 gen🟡 news🟡 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `BZ` | 75 | $18.50 | $2.21 | — | $5,697.35 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.8; leftover $1397.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟡 ab🟢 peer🟢 heat🔴 vol🟢 buy🟢 |
| 2026-08-28 09:30 ET | **BUY** | `SEDG` | 41 | $33.78 | $2.11 | — | $4,310.26 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-4.9; leftover $1397.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🔴 peer🟢 heat🟡 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `SMTC` | 9 | $149.40 | $2.02 | — | $2,963.64 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ⚪; ret5=-11.6; leftover $1397.65 | join🟢 sector🟢 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `URBN` | 16 | $82.70 | $2.04 | — | $1,638.40 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; 🔵; ret5=-4.6; leftover $1397.65 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-08-28 09:30 ET | **BUY** | `ERAS` | 72 | $19.30 | $2.21 | — | $246.60 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer; ret5=-4.1; leftover $1397.65 | join🔴 sector🔴 gen🟡 news🟢 digest🟢 ab🔴 peer🟢 heat🔴 vol🟢 buy🟡 |
| 2026-08-31 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.60 | ▼ 09:30 equity $9,411.64 vs yday $9,694.83 (-283.19) | 09:30 open · cash $246.60 (unchanged overnight, no fees) · equity $9,411.64 vs prior close $9,694.83 (-283.19) because holdings re-marked: BMEA×13 yday $1.68 → 09:30 $1.71 +0.39; NPWR×10 yday $1.89 → 09:30 $1.83 -0.60; PUSA×5 yday $3.79 → 09:30 $3.72 -0.35; ALVO×4 yday $4.88 → 09:30 $4.98 +0.40; CAPR×3 yday $10.06 → 09:30 $9.44 -1.86; ZURA×3 yday $5.85 → 09:30 $5.51 -1.02; SUJA×2 yday $9.00 → 09:30 $10.09 +2.18; ANF×9 yday $145.75 → 09:30 $148.67 +26.28; BHVN×82 yday $16.12 → 09:30 $15.44 -55.76; BZ×75 yday $18.00 → 09:30 $17.89 -8.25; SEDG×41 yday $33.51 → 09:30 $31.50 -82.41; SMTC×9 yday $142.43 → 09:30 $133.04 -84.51; URBN×16 yday $78.79 → 09:30 $81.09 +36.80; ERAS×72 yday $19.49 → 09:30 $17.90 -114.48 | — |
| 2026-09-01 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $246.60 | ▼ 09:30 equity $9,337.96 vs yday $9,401.04 (-63.08) | 09:30 open · cash $246.60 (unchanged overnight, no fees) · equity $9,337.96 vs prior close $9,401.04 (-63.08) because holdings re-marked: BMEA×13 yday $1.71 → 09:30 $1.65 -0.78; NPWR×10 yday $1.82 → 09:30 $1.78 -0.40; PUSA×5 yday $3.80 → 09:30 $3.93 +0.65; ALVO×4 yday $4.96 → 09:30 $5.24 +1.12; CAPR×3 yday $9.36 → 09:30 $10.43 +3.21; ZURA×3 yday $5.64 → 09:30 $5.60 -0.12; SUJA×2 yday $10.09 → 09:30 $9.31 -1.56; ANF×9 yday $149.28 → 09:30 $142.47 -61.29; BHVN×82 yday $15.40 → 09:30 $15.45 +4.10; BZ×75 yday $17.90 → 09:30 $17.37 -39.75; SEDG×41 yday $31.27 → 09:30 $32.22 +38.95; SMTC×9 yday $132.54 → 09:30 $131.65 -8.01; URBN×16 yday $81.09 → 09:30 $80.69 -6.40; ERAS×72 yday $17.90 → 09:30 $18.00 +7.20 | — |
| 2026-09-01 09:30 ET | **SELL** | `BMEA` | 13 | $1.65 | $0.27 | $-0.13 | $267.78 | ▼ -0.13 after sell → book $9,337.69; vs 09:30 mark -0.27 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `NPWR` | 10 | $1.78 | $0.23 | $-2.66 | $285.35 | ▼ -2.66 after sell → book $9,337.46; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `PUSA` | 5 | $3.93 | $0.23 | $+0.72 | $304.77 | ▲ +0.72 after sell → book $9,337.23; vs 09:30 mark -0.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ALVO` | 4 | $5.24 | $0.24 | $-0.38 | $325.48 | ▼ -0.38 after sell → book $9,336.98; vs 09:30 mark -0.25 | dropped from list after 5 sess (min 5) | join🟢 sector🔴 gen🔴 news🟢 digest🟢 judge🟢 ab🟢 peer🔴 heat🔴 vol🔴 buy🟡 |
| 2026-09-01 09:30 ET | **SELL** | `CAPR` | 3 | $10.43 | $0.34 | $+10.37 | $356.43 | ▲ +10.37 after sell → book $9,336.64; vs 09:30 mark -0.34 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `ZURA` | 3 | $5.60 | $0.20 | $-2.74 | $373.04 | ▼ -2.74 after sell → book $9,336.45; vs 09:30 mark -0.19 | dropped from list after 5 sess (min 5) | — |
| 2026-09-01 09:30 ET | **SELL** | `SUJA` | 2 | $9.31 | $0.21 | $+0.65 | $391.44 | ▲ +0.65 after sell → book $9,336.23; vs 09:30 mark -0.22 | dropped from list after 5 sess (min 5) | join🟡 sector🟡 gen🔴 news🟡 digest🟢 ab🟡 heat🔴 vol🔴 buy🟡 |
| 2026-09-02 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $391.44 | ▼ 09:30 equity $9,215.19 vs yday $9,267.83 (-52.64) | 09:30 open · cash $391.44 (unchanged overnight, no fees) · equity $9,215.19 vs prior close $9,267.83 (-52.64) because holdings re-marked: ANF×9 yday $143.00 → 09:30 $142.00 -9.00; BHVN×82 yday $15.45 → 09:30 $15.39 -4.92; BZ×75 yday $17.17 → 09:30 $17.29 +9.00; SEDG×41 yday $31.80 → 09:30 $31.87 +2.87; SMTC×9 yday $129.50 → 09:30 $127.63 -16.83; URBN×16 yday $80.69 → 09:30 $79.12 -25.12; ERAS×72 yday $17.70 → 09:30 $17.58 -8.64 | — |
| 2026-09-03 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $391.44 | ▲ 09:30 equity $9,291.08 vs yday $9,262.37 (+28.71) | 09:30 open · cash $391.44 (unchanged overnight, no fees) · equity $9,291.08 vs prior close $9,262.37 (+28.71) because holdings re-marked: ANF×9 yday $140.68 → 09:30 $139.65 -9.27; BHVN×82 yday $15.74 → 09:30 $15.97 +18.86; BZ×75 yday $17.55 → 09:30 $17.65 +7.50; SEDG×41 yday $32.49 → 09:30 $32.42 -2.87; SMTC×9 yday $132.27 → 09:30 $133.00 +6.57; URBN×16 yday $79.29 → 09:30 $78.84 -7.20; ERAS×72 yday $16.76 → 09:30 $16.97 +15.12 | — |
| 2026-09-03 09:30 ET | **BUY** | `GPRO` | 40 | $1.22 | $0.61 | — | $342.03 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $48.93 | join🔴 sector🟢 gen🟡 news🟡 digest🟢 judge🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `FRVO` | 2 | $18.40 | $0.37 | — | $304.86 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $48.93 | join🔴 sector🟡 gen🟡 news🟡 digest🟢 ab🔴 heat🔴 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRK` | 3 | $15.70 | $0.48 | — | $257.28 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $48.93 | join🟢 sector🔴 gen🟡 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-03 09:30 ET | **BUY** | `MMED` | 2 | $22.78 | $0.46 | — | $211.26 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $48.93 | join🟢 sector🟡 gen🟡 news🟢 digest🟢 judge🟢 ab🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CTMX` | 13 | $3.72 | $0.52 | — | $162.38 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $48.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-03 09:30 ET | **BUY** | `CRDL` | 22 | $2.16 | $0.54 | — | $114.32 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $48.93 | join🟢 sector🟡 gen🟡 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **OPEN** | 09:30 open | — | — | — | — | $114.32 | ▲ 09:30 equity $9,290.58 vs yday $9,258.07 (+32.51) | 09:30 open · cash $114.32 (unchanged overnight, no fees) · equity $9,290.58 vs prior close $9,258.07 (+32.51) because holdings re-marked: ANF×9 yday $136.60 → 09:30 $137.70 +9.90; BHVN×82 yday $15.69 → 09:30 $15.89 +16.40; BZ×75 yday $17.30 → 09:30 $17.31 +0.75; SEDG×41 yday $33.98 → 09:30 $33.69 -11.89; SMTC×9 yday $133.85 → 09:30 $133.10 -6.75; URBN×16 yday $78.75 → 09:30 $79.93 +18.88; ERAS×72 yday $16.37 → 09:30 $16.38 +0.72; GPRO×40 yday $1.69 → 09:30 $1.78 +3.60; FRVO×2 yday $17.98 → 09:30 $18.27 +0.58; CRK×3 yday $15.54 → 09:30 $15.45 -0.27; MMED×2 yday $23.76 → 09:30 $23.88 +0.24; CTMX×13 yday $3.72 → 09:30 $3.73 +0.13; CRDL×22 yday $2.17 → 09:30 $2.18 +0.22 | — |
| 2026-09-04 09:30 ET | **SELL** | `ANF` | 9 | $137.70 | $2.04 | $-67.05 | $1,351.58 | ▼ -67.05 after sell → book $9,288.54; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BHVN` | 82 | $15.89 | $2.26 | $-91.42 | $2,652.30 | ▼ -91.42 after sell → book $9,286.28; vs 09:30 mark -2.26 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `BZ` | 75 | $17.31 | $2.24 | $-93.70 | $3,948.31 | ▼ -93.70 after sell → book $9,284.04; vs 09:30 mark -2.24 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SEDG` | 41 | $33.69 | $2.13 | $-7.94 | $5,327.47 | ▼ -7.94 after sell → book $9,281.91; vs 09:30 mark -2.13 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `SMTC` | 9 | $133.10 | $2.04 | $-150.75 | $6,523.33 | ▼ -150.75 after sell → book $9,279.87; vs 09:30 mark -2.04 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `URBN` | 16 | $79.93 | $2.06 | $-48.42 | $7,800.15 | ▼ -48.42 after sell → book $9,277.81; vs 09:30 mark -2.06 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **SELL** | `ERAS` | 72 | $16.38 | $2.23 | $-214.67 | $8,977.28 | ▼ -214.67 after sell → book $9,275.58; vs 09:30 mark -2.23 | dropped from list after 5 sess (min 5) | — |
| 2026-09-04 09:30 ET | **BUY** | `CABA` | 353 | $3.63 | $4.55 | — | $7,691.34 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1282.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟢 |
| 2026-09-04 09:30 ET | **BUY** | `BAK` | 657 | $1.95 | $8.48 | — | $6,401.71 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1282.47 | join🔴 sector🔴 gen🟢 news🟢 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `EOSE` | 359 | $3.57 | $4.63 | — | $5,115.45 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1282.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `DELL` | 2 | $486.31 | $2.00 | — | $4,140.84 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1282.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `MLYS` | 43 | $29.15 | $2.12 | — | $2,885.27 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1282.47 | join🟢 sector🟢 gen🟢 news🟡 digest🟢 judge🟢 ab🟢 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `CCOI` | 125 | $10.22 | $2.37 | — | $1,605.40 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1282.47 | join🔴 sector🟢 gen🟢 news🟡 digest🟡 ab🔴 peer🟢 heat🟢 vol🟢 buy🟡 |
| 2026-09-04 09:30 ET | **BUY** | `SGLD` | 197 | $6.48 | $2.58 | — | $326.26 | — | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $1282.47 | join🔴 sector🔴 gen🟢 news🟡 digest🟢 ab🟢 heat🟢 vol🟢 buy🟡 |

## Not taken

| Date | Ticker | Kind | Why |
|---|---|---|---|
| 2026-08-17 | `BTBT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `BETR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ANGX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `HYLN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ADUR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `ARX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `AIRO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `NCMI` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-17 | `TMC` | cash | leftover split 1.28 < 1 share @ 4.05 |
| 2026-08-17 | `CDNL` | cash | leftover split 1.28 < 1 share @ 39.85 |
| 2026-08-17 | `ABX` | cash | leftover split 1.28 < 1 share @ 9.12 |
| 2026-08-17 | `VERA` | cash | leftover split 1.28 < 1 share @ 31.30 |
| 2026-08-17 | `CAPR` | cash | leftover split 1.28 < 1 share @ 6.87 |
| 2026-08-17 | `HTFL` | cash | leftover split 1.28 < 1 share @ 41.23 |
| 2026-08-17 | `UMAC` | cash | leftover split 1.28 < 1 share @ 32.55 |
| 2026-08-17 | `NPWR` | cash | leftover split 1.28 < 1 share @ 1.92 |
| 2026-08-18 | `BTBT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `BETR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `HYLN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ADUR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `ARX` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `AIRO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NCMI` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-18 | `NXE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `WFF` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FDMT` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `OABI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `AXTI` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `HIVE` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-18 | `FIGR` | hard_red | hard-red S=-6.20 sit; no new buys |
| 2026-08-19 | `BTBT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `BETR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ANGX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `HYLN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ADUR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `ARX` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `AIRO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `NCMI` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-19 | `OBE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `BBWI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `RANI` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `SECZ` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `AMLX` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `WEAV` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `HAE` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-19 | `DUOT` | hard_red | hard-red S=-7.20 sit; no new buys |
| 2026-08-20 | `BTBT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `BETR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ANGX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `HYLN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ADUR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `ARX` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AIRO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `NCMI` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-20 | `AG` | cash | leftover split 1.28 < 1 share @ 20.55 |
| 2026-08-20 | `BHP` | cash | leftover split 1.28 < 1 share @ 91.01 |
| 2026-08-20 | `CDE` | cash | leftover split 1.28 < 1 share @ 20.65 |
| 2026-08-20 | `HDSN` | cash | leftover split 1.28 < 1 share @ 5.77 |
| 2026-08-20 | `IAG` | cash | leftover split 1.28 < 1 share @ 19.63 |
| 2026-08-20 | `KGC` | cash | leftover split 1.28 < 1 share @ 29.63 |
| 2026-08-20 | `NFGC` | cash | leftover split 1.28 < 1 share @ 1.75 |
| 2026-08-20 | `WPM` | cash | leftover split 1.28 < 1 share @ 144.54 |
| 2026-08-24 | `AU` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AEM` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `ARCT` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `AUTL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CRSP` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `CYPH` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-24 | `RZLT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `CRML` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `LAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `DNN` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `USAR` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `BKKT` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `TMC` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-24 | `GUTS` | hard_red | hard-red S=-5.17 sit; no new buys |
| 2026-08-25 | `AU` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUPH` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AEM` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `ARCT` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `AUTL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRDL` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-25 | `CRSP` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-26 | `AU` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AEM` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `ARCT` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `AUTL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRDL` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CRSP` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `CYPH` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-26 | `BMEA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `NPWR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `PUSA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ALVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `CAPR` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `ZURA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SUJA` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-26 | `SLQT` | no_price | no 09:30 open |
| 2026-08-26 | `USDE` | no_price | no 09:30 open |
| 2026-08-26 | `DKS` | no_price | no 09:30 open |
| 2026-08-27 | `AU` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AEM` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `ARCT` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `AUTL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRDL` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CRSP` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `CYPH` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-27 | `BMEA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `NPWR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `PUSA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ALVO` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `CAPR` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `ZURA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-27 | `SUJA` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-08-28 | `BMEA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `NPWR` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `PUSA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ALVO` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `ZURA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-28 | `SUJA` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-08-31 | `BMEA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `NPWR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `PUSA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ALVO` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `CAPR` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ZURA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `SUJA` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-08-31 | `ANF` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BHVN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `BZ` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SEDG` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `SMTC` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `URBN` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `ERAS` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-08-31 | `OKTA` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRM` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CRWD` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `VEEV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `SNPS` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `USDE` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `DFDV` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-08-31 | `CYPH` | hard_red | hard-red S=-5.85 sit; no new buys |
| 2026-09-01 | `ANF` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BHVN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `BZ` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SEDG` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `SMTC` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `URBN` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-01 | `ERAS` | min_hold | dropped but min-hold 2/5 sess — no sell |
| 2026-09-02 | `ANF` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BHVN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `BZ` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SEDG` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `SMTC` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `URBN` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `ERAS` | min_hold | dropped but min-hold 3/5 sess — no sell |
| 2026-09-02 | `GPRO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `FRVO` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-02 | `YDDL` | hard_red | hard-red S=-3.83 sit; no new buys |
| 2026-09-03 | `ANF` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BHVN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `BZ` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SEDG` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `SMTC` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `URBN` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `ERAS` | min_hold | dropped but min-hold 4/5 sess — no sell |
| 2026-09-03 | `RVTY` | cash | leftover split 48.93 < 1 share @ 125.94 |
| 2026-09-03 | `EIX` | cash | leftover split 48.93 < 1 share @ 56.78 |
| 2026-09-04 | `FRVO` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRK` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `MMED` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CTMX` | min_hold | dropped but min-hold 1/5 sess — no sell |
| 2026-09-04 | `CRDL` | min_hold | dropped but min-hold 1/5 sess — no sell |

## Still open (marked at last close)

| Ticker | Shares | Entry | Why |
|---|---:|---|---|
| `GPRO` | 40 | 2026-09-03 @ $1.22 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+5.9; leftover $48.93 |
| `FRVO` | 2 | 2026-09-03 @ $18.40 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=-14.4; leftover $48.93 |
| `CRK` | 3 | 2026-09-03 @ $15.70 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover,mover_buy; 🔵; ret5=+1.1; leftover $48.93 |
| `MMED` | 2 | 2026-09-03 @ $22.78 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+1.5; leftover $48.93 |
| `CTMX` | 13 | 2026-09-03 @ $3.72 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer; 🔵; ret5=-2.4; leftover $48.93 |
| `CRDL` | 22 | 2026-09-03 @ $2.16 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,mover_buy; 🔵; ⚪; ret5=+3.3; leftover $48.93 |
| `CABA` | 353 | 2026-09-04 @ $3.63 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list flatten; 🔵; ⚪; ret5=+13.8; leftover $1282.47 |
| `BAK` | 657 | 2026-09-04 @ $1.95 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; ret5=+2.1; leftover $1282.47 |
| `EOSE` | 359 | 2026-09-04 @ $3.57 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-5.2; leftover $1282.47 |
| `DELL` | 2 | 2026-09-04 @ $486.31 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=-9.9; leftover $1282.47 |
| `MLYS` | 43 | 2026-09-04 @ $29.15 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ⚪; ret5=+0.0; leftover $1282.47 |
| `CCOI` | 125 | 2026-09-04 @ $10.22 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list probable,yday_gainer,yday_mover; 🔵; ret5=-18.9; leftover $1282.47 |
| `SGLD` | 197 | 2026-09-04 @ $6.48 | union ∩ vol_g hold 5, no 🚨; gate vol=good; list yday_gainer,yday_mover; ret5=+0.0; leftover $1282.47 |
